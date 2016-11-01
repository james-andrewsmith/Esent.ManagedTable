using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

using Microsoft.Database.Isam;
using Microsoft.Database.Isam.Config;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Windows7;
using Microsoft.Isam.Esent.Interop.Vista;

namespace EsentTempTableTest
{
    public abstract class ManagedTable<TConfig, TCursor> : IDisposable 
                                           where TConfig : ManagedTableConfig
                                           where TCursor : ManagedTableCursor<TConfig>
    {        
        // todo:
        // Instead of "TConfig" and abstract methods
        // perhaps a set of "readonly Action<T> or Funct<T, V>" would be better
        // these can be set at the top of the inheriting class tree and would 
        // be passed in as variables?



        public ManagedTable(TConfig config, 
                            Func<Instance, TConfig, TCursor> OpenCursor)
        {
            // To ensure all references are valid
            _config = config;

            // Prepare locks
            _updateLocks = new object[NumUpdateLocks];
            for (int i = 0; i < _updateLocks.Length; ++i)
                _updateLocks[i] = new object();

            // Get the configuration
            var defaultConfig = ManagedTableDefaultConfig.GetDefaultDatabaseConfig();
            var databaseConfig = new DatabaseConfig();

            var databaseDirectory = Environment.CurrentDirectory;
            var databasePath = Path.Combine(databaseDirectory, defaultConfig.DatabaseFilename);
            // databaseConfig.DatabaseFilename = databasePath;
            databaseConfig.SystemPath = databaseDirectory;
            databaseConfig.LogFilePath = databaseDirectory;
            databaseConfig.TempPath = databaseDirectory;

            // Apply configuration            
            databaseConfig.Merge(defaultConfig);
            databaseConfig.Merge(_config.GetDefaultDatabaseConfig(), MergeRules.Overwrite);
            databaseConfig.SetGlobalParams();
             
            // Get the database instance
            _instance = new Instance(
                databaseConfig.Identifier, 
                databaseConfig.DisplayName, 
                databaseConfig.DatabaseStopFlags
            );

            // Apply instance level config
            databaseConfig.SetInstanceParams(_instance.JetInstance);

            // Todo: look for fastest recovery flags
            InitGrbit grbit = databaseConfig.DatabaseRecoveryFlags | (EsentVersion.SupportsWindows7Features ? Windows7Grbits.ReplayIgnoreLostLogs : InitGrbit.None);
            _instance.Init(grbit);

            // 
            try
            {
                
                _config.Database = databaseConfig.DatabaseFilename;
                using (var session = new Session(_instance))
                {
                    JET_DBID dbid;
                    Api.JetCreateDatabase2(
                        session, 
                        databasePath, 
                        databaseConfig.DatabaseMaxPages, 
                        out dbid, 
                        databaseConfig.DatabaseCreationFlags | 
                        CreateDatabaseGrbit.OverwriteExisting | 
                        CreateDatabaseGrbit.RecoveryOff
                    );


                    try
                    {

                        // Abstract function 
                        using (var transaction = new Transaction(session))
                        {
                            CreateManagedTable(session, dbid);

                            transaction.Commit(CommitTransactionGrbit.None);
                            Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                        }
                    }
                    catch(Exception)
                    {
                        // Delete the partially constructed database
                        Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                        Api.JetDetachDatabase(session, databasePath);
                        File.Delete(databasePath);
                        throw;
                    }
                }

                // Get the cursor cache
                _cursors = new ManagedTableCursorCache<TConfig, TCursor>(
                    _instance,
                    _config,
                    OpenCursor
                );
            }
            catch (Exception)
            {
                // We have failed to initialize for some reason. Terminate
                // the instance.
                _instance.Term();
                throw;
            }

            // Now attach the database to the instance and held variable
            _database = new Database(_instance.JetInstance, false, databaseConfig);             
        }


        private bool _disposed;
        protected const int NumUpdateLocks = 31;
        protected readonly object[] _updateLocks;
        protected readonly ReaderWriterLockSlim _disposeLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        protected readonly ManagedTableCursorCache<TConfig, TCursor> _cursors;

        protected readonly TConfig _config;
        protected readonly Instance _instance;
        protected readonly Database _database; 



        protected abstract void CreateManagedTable(Session session, JET_DBID dbid);


        /// <summary>
        /// Performs the specified action while under a ReadLock.
        /// </summary>
        /// <param name="action">The action to perform.</param>
        internal void DoReadLockedOperation(Action action)
        {
            this.CheckObjectDisposed();

            try
            {
                _disposeLock.EnterReadLock();
                CheckObjectDisposed();

                action();
            }
            finally
            {
                _disposeLock.ExitReadLock();
            }
        }


        /// <summary>
        /// Performs the specified action while under a ReadLock. This is usually done to
        /// prevent the underlying dictionary from being Dispose'd from underneath us.
        /// </summary>
        /// <param name="action">The action to perform.</param>
        /// <typeparam name="TReturn">The type of the return value of the block.</typeparam>
        /// <returns>Returns the value of the function.</returns>
        internal TReturn ReturnReadLockedOperation<TReturn>(Func<TReturn> action)
        {
            this.CheckObjectDisposed();

            try
            {
                _disposeLock.EnterReadLock();
                CheckObjectDisposed();

                return action();
            }
            finally
            {
                _disposeLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Verifies that the object is not already disposed.
        /// This should be checked while the disposeLock ReadLock is held. If
        /// the read lock is not held, then the caller must acquire the lock
        /// first and check for a guaranteed correct value.
        /// (The caller may call this without the lock first to get a fast-but-
        /// inaccurate result).
        /// </summary>
        /// <exception cref="ObjectDisposedException">
        /// Thrown if the object has already been disposed.
        /// </exception>
        private void CheckObjectDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("PersistentDictionary");
            }
        }

        /// <summary>
        /// Gets an object used to lock updates to the key.
        /// </summary>
        /// <param name="normalizedKey">The normalized key to be locked.</param>
        /// <returns>
        /// An object that should be locked when the key is updated.
        /// </returns>
        protected object LockObject(byte[] normalizedKey)
        {
            if (null == normalizedKey)
            {
                return _updateLocks[0];
            }

            // Remember: hash codes can be negative, and we can't negate Int32.MinValue.
            uint hash = unchecked((uint)GetHashCodeForKey(normalizedKey));
            hash %= checked((uint)_updateLocks.Length);

            return _updateLocks[checked((int)hash)];
        }

        /// <summary>
        /// Calculates a hash code for the normalized key.
        /// </summary>
        /// <param name="normalizedKey">A byte array.</param>
        /// <returns>A hash code based on the byte array.</returns>
        /// <remarks>
        /// This is similar to Store's IEqualityComparer&lt;object[]&gt;.GetHashCode(object[] x).
        /// It is not meant to be cryptographically secure.
        /// </remarks>
        public static int GetHashCodeForKey(byte[] normalizedKey)
        {
            // This is similar to Store's IEqualityComparer<object[]>.GetHashCode(object[] x)

            // XOR-together the hash code 4-bytes at a time.
            int hashCode = normalizedKey.Length;
            for (int i = 0; i < normalizedKey.Length; ++i)
            {
                hashCode ^= normalizedKey[i];

                // Rotate the hash by one bit so that arrays with the same
                // elements in a different order will have different hash
                // values
                hashCode = hashCode << 1 | hashCode >> 31;
            }

            return hashCode;
        }

        /// <summary>
        /// Determine if the given column can be compressed.
        /// </summary>
        /// <param name="columndef">The definition of the column.</param>
        /// <returns>True if the column can be compressed.</returns>
        private static bool ColumnCanBeCompressed(JET_COLUMNDEF columndef)
        {
            return EsentVersion.SupportsWindows7Features
                   && (JET_coltyp.LongText == columndef.coltyp || JET_coltyp.LongBinary == columndef.coltyp);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="userInitiatedDisposing">Whether it's a user-initiated call.</param>
        private void Dispose(bool userInitiatedDisposing)
        {
            if (_disposed)
            {
                return;
            }

            if (userInitiatedDisposing)
            {
                // Indicates a coding error.
                Debug.Assert(!_disposeLock.IsReadLockHeld, "No read lock should be held when Disposing this object.");
                Debug.Assert(!_disposeLock.IsWriteLockHeld, "No read lock should be held when Disposing this object.");

                bool writeLocked = false;
                try
                {
                    _disposeLock.EnterWriteLock();
                    writeLocked = true;

                    if (_disposed)
                    {
                        return;
                    }

                    _cursors.Dispose();
                    _database.Dispose();
                    _instance.Dispose();
                }
                finally
                {
                    _disposed = true;
                    if (writeLocked)
                    {
                        _disposeLock.ExitWriteLock();
                    }

                    // Can't Dipose it when other threads may be blocked on it,
                    // trying to enter as Readers.
                    //// this.disposeLock.Dispose();
                }
            }
        }
         


        // A function to 

        /// <summary>
        /// Invokes the Dispose(bool) function.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }


    }
}
