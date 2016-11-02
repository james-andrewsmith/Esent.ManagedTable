using System;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using Microsoft.Database.Isam;
using Microsoft.Database.Isam.Config;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows7;
using Microsoft.Isam.Esent.Interop.Vista;

namespace Esent.ManagedTable
{
    public class SubscriberTable : ManagedTable<SubscriberConfig, SubscriberCursor>
    {
        public SubscriberTable() : base(new SubscriberConfig(), OpenCursor)
        {            
        }

        private static Func<Instance, SubscriberConfig, SubscriberCursor> OpenCursor = 
            (instance, config) => { return new SubscriberCursor(instance, config); };

        /// <summary>
        /// This whole function is called from within a tranaction
        /// </summary>
        /// <param name="session"></param>
        /// <param name="dbid"></param>
        protected override void CreateManagedTable(Session session, JET_DBID dbid)
        {            
             
            JET_TABLEID tableID;
            var columnids = new JET_COLUMNID[2];

            // Create the table
            Api.JetCreateTable(session, dbid, _config.TableName, 128, 100, out tableID);
            
            // Add the subscriber column which will have the trace request ID
            Api.JetAddColumn(
                session,
                tableID,
                _config.SubscriberColumnName,
                new JET_COLUMNDEF {
                    coltyp = JET_coltyp.LongText,
                    grbit = ColumndefGrbit.TTKey,
                    cp = JET_CP.ASCII
                },
                null,
                0,
                out columnids[0]);

            // Add the multi-valued channel column which has the name of each channel
            // the connection has subscribed to
            Api.JetAddColumn(
                session,
                tableID,
                _config.ChannelColumnName,
                new JET_COLUMNDEF {
                    coltyp = JET_coltyp.LongText,
                    cp = JET_CP.ASCII,
                    grbit = ColumndefGrbit.ColumnMultiValued
                },
                null,
                0,
                out columnids[1]);

            // Prepare key which summarises the index
            string pkIndexKey = string.Format(CultureInfo.InvariantCulture, "+{0}\0\0", _config.SubscriberColumnName);
            string chIndexKey = string.Format(CultureInfo.InvariantCulture, "+{0}\0\0", _config.ChannelColumnName);

            // Request index creation as one operation 
            var indexcreates = new[]
            {
                new JET_INDEXCREATE
                {
                    cbKeyMost = SystemParameters.KeyMost,
                    grbit = CreateIndexGrbit.IndexPrimary,
                    szIndexName = "primary",
                    szKey = pkIndexKey,
                    cbKey = pkIndexKey.Length,
                    pidxUnicode = new JET_UNICODEINDEX
                    {
                        lcid = CultureInfo.CurrentCulture.LCID,
                        dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.OrdinalIgnoreCase),
                    },
                } 
            };
             
            // Create primary index
            Api.JetCreateIndex2(session, tableID, indexcreates, indexcreates.Length);

            // Create the secondary index on the multi-valued column
            Api.JetCreateIndex(
                session, 
                tableID, 
                "channelindex", 
                CreateIndexGrbit.None,
                chIndexKey,
                chIndexKey.Length,
                100
            );

            // Clean up
            Api.JetCloseTable(session, tableID);
        }

        // AddSubscription(subscriber, channel)
        // RemoveSubscription(subscriber, channel)
        // RemoveSubscriber(subcriber)
        // GetSubscribers(channel)

        public void Subscribe(string subscriber, IEnumerable<string> channels)
        {            
            DoReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                cursor.MakeKey(subscriber);
                lock (LockObject(cursor.GetNormalizedKey()))
                {
                    try
                    {
                        using (var transaction = cursor.BeginLazyTransaction())
                        {
                            // If it exists, then ensure the channels are all subscribed
                            // Do not throw an error
                            if (cursor.TrySeek())
                            {
                                // cursor.Set
                                cursor.Append(channels);
                            }
                            // If it does not exist then add the row with all the columns
                            // and then move on
                            else
                            {
                                cursor.Insert(subscriber, channels);
                            }
                                
                            transaction.Commit();
                        }
                    }
                    finally
                    {
                        _cursors.FreeCursor(cursor);
                    }
                }
            });
        }

        public IEnumerable<string> GetSubscribers(string channel)
        {
            // Lookup all subscribers in the index for this channel
            // Return them all
            return ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                try
                {
                    // Start a transaction so the record can't be deleted after
                    // we seek to it. 
                    using (var transaction = cursor.BeginReadOnlyTransaction())
                    {
                        return cursor.GetSubscribersByChannel(channel);
                    }  
                }
                finally
                {
                    _cursors.FreeCursor(cursor);
                }
            }); 
        }
         

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

        public void UnsubscribeAll(string subscriber)
        {
            // Remove row
            ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                cursor.MakeKey(subscriber);
                lock (LockObject(cursor.GetNormalizedKey()))
                {
                    try
                    {
                        // Having the update lock means the record can't be
                        // deleted after we seek to it.
                        if (cursor.TrySeek())
                        {
                            using (var transaction = cursor.BeginLazyTransaction())
                            {
                                cursor.DeleteCurrent();
                                transaction.Commit();
                                return true;
                            }
                        }

                        return false;
                    }
                    finally
                    {
                        _cursors.FreeCursor(cursor);
                    }
                }
            });
        }

        public void Unsubscribe(string subscriber, string channel)
        {
            DoReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                cursor.MakeKey(subscriber);
                lock (LockObject(cursor.GetNormalizedKey()))
                {
                    try
                    {
                        using (var transaction = cursor.BeginLazyTransaction())
                        {
                            // If it exists, then ensure the channels are all subscribed
                            // Do not throw an error
                            if (cursor.TrySeek())
                            {
                                // cursor.Set
                                cursor.RemoveChannel(channel);
                            }                            

                            transaction.Commit();
                        }
                    }
                    finally
                    {
                        _cursors.FreeCursor(cursor);
                    }
                }
            });

            // If row doesn't exist do nothing 

            // Loop through all the mv values until the channel is found
            // set that channel to null
        }
       
       
        public void Subscribe(string subscriber, string channel)
        {
            Subscribe(subscriber, new[] { channel });           
        }

        public IEnumerable<string> GetSubscriberChannels(string subscriber)
        {
            // Lookup all subscribers in the index for this channel
            // Return them all
            return ReturnReadLockedOperation(() =>
            {
                var cursor = _cursors.GetCursor();
                cursor.MakeKey(subscriber);
                lock (LockObject(cursor.GetNormalizedKey()))
                {
                    try
                    {
                        // Start a transaction so the record can't be deleted after
                        // we seek to it. 
                        using (var transaction = cursor.BeginReadOnlyTransaction())
                        {
                            // If it exists, then ensure the channels are all subscribed
                            // Do not throw an error
                            if (cursor.TrySeek())
                            {
                                // cursor.Set
                                return cursor.GetChannelsBySubscriber(subscriber);
                            }
                            // If it does not exist then add the row with all the columns
                            // and then move on
                            else
                            {
                                return Enumerable.Empty<string>();
                            }
                        }
                    }
                    finally
                    {
                        _cursors.FreeCursor(cursor);
                    }
                }
            });
        }

    }
}
