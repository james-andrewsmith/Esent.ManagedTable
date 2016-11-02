using System;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

using Wire;

using Microsoft.Database.Isam;
using Microsoft.Database.Isam.Config;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows7;
using Microsoft.Isam.Esent.Interop.Vista;

namespace Esent.ManagedTable
{
    public class CacheTable : ManagedTable<CacheConfig, CacheCursor>
    {
        public CacheTable(int cacheSize = 64) : base(new CacheConfig(cacheSize), OpenCursor)
        {
            _delegates = new Dictionary<int, CacheEntryEvictionDelegate>();
            _entryLock = new ReaderWriterLockSlim();

            _expirationScanFrequency = TimeSpan.FromMinutes(1);

            _wire = new Serializer(new SerializerOptions(
                knownTypes: new[]
                {
                    typeof(int),
                    typeof(int[]),
                    typeof(long),
                    typeof(long[])
                }                
            ));

            _ss = _wire.GetSerializerSession();
            _ds = _wire.GetDeserializerSession();
        }

        private static Func<Instance, CacheConfig, CacheCursor> OpenCursor = 
            (instance, config) => { return new CacheCursor(instance, config); };

        private readonly Dictionary<int, CacheEntryEvictionDelegate> _delegates;
        private readonly ReaderWriterLockSlim _entryLock;
        private readonly TimeSpan _expirationScanFrequency;

        
        private DateTimeOffset _lastExpirationScan;


        private readonly Serializer _wire;
        private readonly SerializerSession _ss;
        private readonly DeserializerSession _ds;


        /// <summary>
        /// This whole function is called from within a tranaction
        /// </summary>
        /// <param name="session"></param>
        /// <param name="dbid"></param>
        protected override void CreateManagedTable(Session session, JET_DBID dbid)
        {            
             
            JET_TABLEID tableID;
            var columnids = new JET_COLUMNID[7];

            // Create the table
            Api.JetCreateTable(session, dbid, _config.TableName, 128, 100, out tableID);
            
            // The key 
            Api.JetAddColumn(
                session,
                tableID,
                _config.KeyColumnName,
                new JET_COLUMNDEF {
                    coltyp = JET_coltyp.LongText,
                    grbit = ColumndefGrbit.TTKey,
                    cp = JET_CP.Unicode
                },
                null,
                0,
                out columnids[0]
            );

            // A binary field with the actual data stored in it, serialised by wire
            // and storing large stuff typically
            Api.JetAddColumn(
                session,
                tableID,
                _config.DataColumnName,
                new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.LongBinary
                },
                null,
                0,
                out columnids[1]);

            // Add the multi-valued channel column which has the name of each channel
            // the connection has subscribed to
            Api.JetAddColumn(
                session,
                tableID,
                _config.DependencyColumnName,
                new JET_COLUMNDEF {
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.ColumnMultiValued
                },
                null,
                0,
                out columnids[2]);

            // Add the multi-valued channel column which has the name of each channel
            // the connection has subscribed to
            Api.JetAddColumn(
                session,
                tableID,
                _config.CallbacksColumnName,
                new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.Binary
                },
                null,
                0,
                out columnids[3]);

            Api.JetAddColumn(
                session,
                tableID,
                _config.LastAccessedColumnName,
                new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.Currency
                },
                null,
                0,
                out columnids[4]);


            Api.JetAddColumn(
                session,
                tableID,
                _config.SlidingExpirationColumnName,
                new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.Long
                },
                null,
                0,
                out columnids[5]
            );

            Api.JetAddColumn(
                session,
                tableID,
                _config.AbsoluteExpirationColumnName,
                new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.Currency
                },
                null,
                0,
                out columnids[6]
            );

            var idPrimaryKey = string.Format(CultureInfo.InvariantCulture, "+{0}\0\0", _config.KeyColumnName);
            var idDependency = string.Format(CultureInfo.InvariantCulture, "+{0}\0\0", _config.DependencyColumnName);
            var idSliding = string.Format(
                CultureInfo.InvariantCulture, 
                "+{0}\0+{1}\0\0", 
                _config.LastAccessedColumnName,
                _config.SlidingExpirationColumnName
            );
            var idAbsolute = string.Format(
                CultureInfo.InvariantCulture, 
                "+{0}\0\0", 
                _config.AbsoluteExpirationColumnName
            );            
             
            // Create primary index
            Api.JetCreateIndex2(session, tableID, new[] {
                new JET_INDEXCREATE
                {
                    cbKeyMost = SystemParameters.KeyMost,
                    grbit = CreateIndexGrbit.IndexPrimary,
                    szIndexName = _config.PrimaryKeyIndexName,
                    szKey = idPrimaryKey,
                    cbKey = idPrimaryKey.Length,
                    pidxUnicode = new JET_UNICODEINDEX
                    {
                        lcid = CultureInfo.CurrentCulture.LCID,
                        dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.OrdinalIgnoreCase),
                    },
                }
            }, 1);

            Api.JetCreateIndex2(session, tableID, new[] {
                new JET_INDEXCREATE
                {
                    cbKeyMost = SystemParameters.KeyMost,
                    grbit = CreateIndexGrbit.IndexIgnoreAnyNull,
                    szIndexName = _config.SlidingExpiryIndexName,
                    szKey = idSliding,
                    cbKey = idSliding.Length
                }
            }, 1);

            Api.JetCreateIndex2(session, tableID, new[] {
                new JET_INDEXCREATE
                {
                    cbKeyMost = SystemParameters.KeyMost,
                    grbit = CreateIndexGrbit.IndexIgnoreAnyNull,
                    szIndexName = _config.AbsoluteExpiryIndexName,
                    szKey = idAbsolute,
                    cbKey = idAbsolute.Length
                }
            }, 1);


            // Create the secondary index on the multi-valued column
            Api.JetCreateIndex(
                session, 
                tableID, 
                _config.DependencyIndexName, 
                CreateIndexGrbit.None,
                idDependency,
                idDependency.Length,
                100
            );

            // Clean up
            Api.JetCloseTable(session, tableID);
        }

        public void Set(long key, 
                        byte[] data, 
                        CacheEntryOptions options = null)
        {
            Set(key.ToString(), data, options);
        }
         
        public void Set(string key, 
                        byte[] data, 
                        CacheEntryOptions options = null)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            if (data == null)
                throw new ArgumentNullException(nameof(data));
            
            int[] callbacks = null;
            string[] dependencies = null;

            int slidingExpiration = 0;
            long absoluteExpiration = 0;
            var utcNow = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            if (options != null)
            {
                callbacks = new int[options.PostEvictionCallbacks.Count];
                for (int i = 0; i < options.PostEvictionCallbacks.Count; i++)
                {
                    var token = options.PostEvictionCallbacks[i].Method.MetadataToken;
                    callbacks[i] = token;
                    _entryLock.EnterUpgradeableReadLock();
                    if (!_delegates.ContainsKey(token))
                    {
                        _entryLock.EnterWriteLock();
                        _delegates.Add(token, options.PostEvictionCallbacks[i]);
                        _entryLock.ExitWriteLock();
                    }
                    _entryLock.ExitUpgradeableReadLock();                    
                }

                dependencies = new string[options.Dependencies.Count];
                for(int i = 0; i < options.Dependencies.Count; i++)
                {
                    dependencies[i] = options.Dependencies[i];
                }
            }
            
            if (options.AbsoluteExpirationRelativeToNow.HasValue)
            {
                absoluteExpiration = utcNow + Convert.ToInt64(options.AbsoluteExpirationRelativeToNow.Value.TotalSeconds);
            }
            else if (options.AbsoluteExpiration.HasValue)
            {
                absoluteExpiration = options.AbsoluteExpiration.Value.ToUnixTimeSeconds();
            }
            else if (options.SlidingExpiration.HasValue)
            {
                slidingExpiration = Convert.ToInt32(options.SlidingExpiration.Value.TotalSeconds); 
            } 

            CacheEntry entry = new CacheEntry();
            entry.Key = key;
            entry.Data = data;
            entry.Dependencies = dependencies;

            using (var ms = new System.IO.MemoryStream())
            {
                _wire.Serialize(callbacks, ms, _ss);
                entry.PostEvicationCallbacks = ms.ToArray();
            }
            
            entry.LastAccessed = utcNow;
            entry.SlidingExpiration = slidingExpiration;
            entry.AbsoluteExpiration = absoluteExpiration;

            // Todo:
            // actually check if it should be added
            var added = true;
            
            // todo:
            // if we're not adding still run a "check and expire" operation instead

            CacheEntry existing = ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();

                

                cursor.MakeKey(entry.Key);
                lock (LockObject(cursor.GetNormalizedKey()))
                {
                    try
                    {
                        CacheEntry previous = null;
                        using (var transaction = cursor.BeginLazyTransaction())
                        {
                            // If it exists, then ensure the channels are all subscribed
                            // Do not throw an error
                            if (cursor.TrySeek())
                            {
                                // Get the basic details used in a callback, perhaps not the full
                                // entry for performance, not sure yet
                                previous = cursor.GetEntryForCallback();
                                cursor.Replace(entry);
                            }
                            // If it does not exist then add the row with all the columns
                            // and then move on
                            else
                            {
                                cursor.Insert(entry);
                            }

                            transaction.Commit();
                        }
                        return previous;
                    }
                    finally
                    {
                        _cursors.FreeCursor(cursor);
                    }
                }
            });
            
            if (existing != null && 
                existing.PostEvicationCallbacks.Length > 0)
            {
                existing._EvictionReason = EvictionReason.Replaced;
                InvokeEvictionCallbacks(existing);
            }

            if (!added)
            {
                entry._EvictionReason = EvictionReason.Expired;
                InvokeEvictionCallbacks(entry);
            }
            
            StartScanForExpiredItems();
        }         

        public CacheEntry Get(string key)
        {
            throw new NotImplementedException();
        }

        public byte[] GetData(long key)
        {
            if (key == 0)
                throw new ArgumentNullException(nameof(key));

            return GetData(key.ToString());
        }

        public byte[] GetData(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            // Lookup all subscribers in the index for this channel
            // Return them all
            return ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                cursor.MakeKey(key);
                lock (LockObject(cursor.GetNormalizedKey()))
                {
                    try
                    {
                        // Start a transaction so the record can't be deleted after
                        // we seek to it. 
                        byte[] data;
                        long lastAccess;
                        using (var transaction = cursor.BeginReadOnlyTransaction())
                        {
                            // If it exists, then ensure the channels are all subscribed
                            // Do not throw an error
                            if (!cursor.TrySeek())
                            {
                                return null;                                
                            }

                            data = cursor.GetData();
                            lastAccess = cursor.GetLastAccess();
                        }

                        // if last access exists, then update it's time
                        if (lastAccess > 0)
                        {
                            using (var transaction = cursor.BeginLazyTransaction())
                            {
                                cursor.SetLastAccess(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                                transaction.Commit();
                            }
                        }

                        return data;
                    }
                    finally
                    {
                        _cursors.FreeCursor(cursor);
                    }
                }
            });
        }

        public bool TryGetData(long key, out byte[] data)
        {
            data = GetData(key);
            return data != null;
        }

        public bool TryGetData(string key, out byte[] data)
        {
            data = GetData(key);
            return data != null;
        }

        public bool TryGet(string key, out CacheEntry data)
        {
            throw new NotImplementedException();
        }

        public void RemoveByDependency(string dependency)
        {
            var removed = ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                try
                {
                    IEnumerable<CacheEntry> entries;
                    using (cursor.BeginReadOnlyTransaction())
                    {
                        entries = cursor.RemoveByDependency(dependency);
                    }

                    foreach (var entry in entries)
                    {
                        cursor.MakeKey(entry.Key);
                        lock (LockObject(cursor.GetNormalizedKey()))
                        {
                            if (cursor.TrySeek())
                                using (var transaction = cursor.BeginLazyTransaction())
                                {
                                    cursor.DeleteCurrent();
                                    transaction.Commit();
                                }
                        }
                    }

                    return entries;
                }
                finally
                {
                    _cursors.FreeCursor(cursor);
                }

            });

            foreach (var entry in removed)
            {
                entry._EvictionReason = EvictionReason.Dependency;
                InvokeEvictionCallbacks(entry);
            }
        }

        public void RemoveByKey(string key)
        {

        }

        private IEnumerable<CacheEntry> RemoveByAbsoluteExpiryLessThan(long epoch)
        { 

            return ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                try
                {
                    IEnumerable<CacheEntry> entries;
                    using (cursor.BeginReadOnlyTransaction())
                    {
                        entries = cursor.RemoveByAbsoluteExpiryLessThan(epoch);
                    }

                    foreach (var entry in entries)
                    {
                        cursor.MakeKey(entry.Key);
                        lock (LockObject(cursor.GetNormalizedKey()))
                        {
                            if (cursor.TrySeek())
                                using (var transaction = cursor.BeginLazyTransaction())
                                {
                                    cursor.DeleteCurrent();
                                    transaction.Commit();
                                }
                        }
                    }

                    return entries;
                }
                finally
                {
                    _cursors.FreeCursor(cursor);
                }

            });
        }

        public IEnumerable<CacheEntry> RemoveBySlidingExpiry(long epoch)
        {
            return ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                try
                {
                    IEnumerable<CacheEntry> entries;
                    using (cursor.BeginReadOnlyTransaction())
                    {
                        entries = cursor.RemoveBySlidingExpiry(epoch);                        
                    }
                    
                    foreach(var entry in entries)
                    {
                        cursor.MakeKey(entry.Key);
                        lock (LockObject(cursor.GetNormalizedKey()))
                        {
                            if (cursor.TrySeek())
                                using (var transaction = cursor.BeginLazyTransaction())
                                {
                                    cursor.DeleteCurrent();
                                    transaction.Commit();
                                }
                        }
                    }

                    return entries;            
                }
                finally
                {
                    _cursors.FreeCursor(cursor);
                }

            });
        }

        /// <summary>
        /// Deletes everything in the cache but does not fire post eviction callbacks 
        /// </summary>
        public void Clear()
        {
            DoReadLockedOperation(() =>
            {
                try
                {
                    // We will be deleting all items so take all the update locks
                    foreach (object lockObject in _updateLocks)
                    {
                        Monitor.Enter(lockObject);
                    }

                    var cursor = _cursors.GetCursor();
                    try
                    {
                        cursor.MoveBeforeFirst();
                        while (cursor.TryMoveNext())
                        {
                            using (var transaction = cursor.BeginLazyTransaction())
                            {
                                cursor.DeleteCurrent();
                                transaction.Commit();
                            }
                        }
                    }
                    finally
                    {
                        _cursors.FreeCursor(cursor);
                    }
                }
                finally
                {
                    // Remember to unlock everything
                    foreach (object lockObject in _updateLocks)
                    {
                        Monitor.Exit(lockObject);
                    }
                }
            });
        }
        

        // Called by multiple actions to see how long it's been since we last checked for expired items.
        // If sufficient time has elapsed then a scan is initiated on a background task.
        internal void StartScanForExpiredItems(bool force = false)
        {
            var now = DateTimeOffset.UtcNow;
            if (_expirationScanFrequency < now - _lastExpirationScan || force)
            {
                _lastExpirationScan = now;
                Task.Factory.StartNew(state => 
                    ScanForExpiredItems((CacheTable)state), 
                    this,
                    CancellationToken.None, 
                    TaskCreationOptions.DenyChildAttach, 
                    TaskScheduler.Default
                );
            }
        }

        private static void ScanForExpiredItems(CacheTable cache)
        {
            var expiredEntries = new List<object>();

            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var entries = cache.RemoveByAbsoluteExpiryLessThan(now).Union(
                          cache.RemoveBySlidingExpiry(now));

            // F


            // For each expired entry yielded fire off the callback
            foreach (var entry in entries)
            {
                entry._EvictionReason = EvictionReason.Expired;
                cache.InvokeEvictionCallbacks(entry);
            }

            /*
            foreach (var entry in cache._entries.Values)
            {
                if (entry.CheckExpired(now))
                {
                    expiredEntries.Add(entry);
                }
            }
            
            
            cache.RemoveEntries(expiredEntries);
            */
        }

        internal void InvokeEvictionCallbacks(CacheEntry entry)
        {

            
            Task.Factory.StartNew(
                InvokeCallbacks, 
                entry,
                CancellationToken.None, 
                TaskCreationOptions.DenyChildAttach, 
                TaskScheduler.Default
            );
            
        }

        private void InvokeCallbacks(object state)
        {
            try
            {
                CacheEntry entry = state as CacheEntry;
                if (entry != null)
                {
                    int[] callbacks;
                    using (var ms = new System.IO.MemoryStream(entry.PostEvicationCallbacks))
                    {
                        callbacks = _wire.Deserialize<int[]>(ms, _ds);                        
                    }

                    // Find each callback and run it 
                    for (int i = 0; i < callbacks.Length; i++)
                    {
                        var callbackID = callbacks[i];
                        if (_delegates.ContainsKey(callbackID))
                        {
                            _delegates[callbackID].Invoke(
                                entry.Key, 
                                entry.Data, 
                                entry._EvictionReason
                            );
                        }
                    }
                }                
            }
            catch (Exception exp)
            {
#if DEBUG
                Console.WriteLine(exp.Message);
#endif 
            }

            /*
            var callbackRegistrations = Interlocked.Exchange(ref entry._postEvictionCallbacks, null);

            if (callbackRegistrations == null)
            {
                return;
            }

            for (int i = 0; i < callbackRegistrations.Count; i++)
            {
                var registration = callbackRegistrations[i];

                try
                {
                    registration.EvictionCallback?.Invoke(entry.Key, entry.Value, entry._evictionReason, registration.State);
                }
                catch (Exception)
                {
                    // This will be invoked on a background thread, don't let it throw.
                    // TODO: LOG
                }
            }
            */
        }

    }
}
