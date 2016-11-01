using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent;


namespace EsentTempTableTest
{
    public abstract class ManagedTableCursor<TConfig> : IDisposable 
                                        where TConfig : ManagedTableConfig
    {
        public ManagedTableCursor(Instance instance,TConfig config)
        {
            _instance = instance;
            _config = config;

            Api.JetBeginSession(_instance, out _sesid, string.Empty, string.Empty);
            Api.JetOpenDatabase(_sesid, _config.Database, string.Empty, out _dbid, OpenDatabaseGrbit.None);
            Api.JetOpenTable(_sesid, _dbid, _config.TableName, null, 0, OpenTableGrbit.None, out _table);             
        }

        // what is the name of the table?
        protected readonly TConfig _config;

        /// <summary>
        /// The ESENT instance the cursor is opened against.
        /// </summary>
        protected readonly Instance _instance;

        /// <summary>
        /// The ESENT session the cursor is using.
        /// </summary>
        public readonly JET_SESID _sesid;

        /// <summary>
        /// ID of the opened database.
        /// </summary>
        protected readonly JET_DBID _dbid;

        /// <summary>
        /// ID of the opened globals table.
        /// </summary>
        protected readonly JET_TABLEID _table;
         
                  
        /// <summary>
        /// Gets the current transaction level of the
        /// <see cref="PersistentDictionaryCursor&lt;TKey, TValue&gt;"/>.
        /// Requires Win10. Otherwise returns -1, since 0 and positive numbers
        /// are legitimate output.
        /// </summary>
        public int TransactionLevel
        {
            get
            {
                int transactionLevel = -1;

                if (EsentVersion.SupportsWindows10Features)
                {
                    Microsoft.Isam.Esent.Interop.Windows8.Windows8Api.JetGetSessionParameter(
                        _sesid,
                        Microsoft.Isam.Esent.Interop.Windows10.Windows10Sesparam.TransactionLevel,
                        out transactionLevel
                    );
                }

                return transactionLevel;
            }
        }

        /// <summary>
        /// Begin a new transaction for this cursor.
        /// </summary>
        /// <returns>The new transaction.</returns>
        public Transaction BeginTransaction()
        {
            return new Transaction(_sesid);
        }

        /// <summary>
        /// Begin a new lazy transaction for this cursor. This is cheaper than
        /// <see cref="BeginTransaction"/> because it returns a struct.
        /// </summary>
        /// <returns>The new transaction.</returns>
        public LazyTransaction BeginLazyTransaction()
        {
            return new LazyTransaction(_sesid);
        }

        /// <summary>
        /// Begin a new transaction for this cursor. This is the cheapest
        /// transaction type because it returns a struct and no separate
        /// commit call has to be made.
        /// </summary>
        /// <returns>The new transaction.</returns>
        public ReadOnlyTransaction BeginReadOnlyTransaction()
        {
            return new ReadOnlyTransaction(_sesid);
        }

        /// <summary>
        /// Calls JetRetrieveKey.
        /// </summary>
        /// <returns>The byte value of the normalized key.</returns> 
        internal byte[] GetNormalizedKey()
        {
            return Api.RetrieveKey(_sesid, _table, RetrieveKeyGrbit.RetrieveCopy);
        }



        /// <summary>
        /// Try to find the specified key. If the key is found
        /// the cursor will be positioned on the record with the
        /// key.
        /// </summary>
        /// <param name="key">The key to find.</param>
        /// <returns>True if the key was found, false otherwise.</returns>
        public bool TrySeek(string key)
        {
            // this.MakeKey(key);
            return Api.TrySeek(_sesid, _table, SeekGrbit.SeekEQ);
        }

        /// <summary>
        /// Try to find the key specified with a previous call to <see cref="MakeKey"/>.
        /// If the key is found
        /// the cursor will be positioned on the record with the
        /// key.
        /// </summary>
        /// <returns>True if the key was found, false otherwise.</returns>
        public bool TrySeek()
        {
            return Api.TrySeek(_sesid, _table, SeekGrbit.SeekEQ);
        }

        /// <summary>
        /// Seek for the specified key. If the key is found the
        /// cursor will be positioned with the record on the key.
        /// If the key is not found then an exception will be thrown.
        /// </summary>
        /// <param name="key">The key to find.</param>
        /// <exception cref="KeyNotFoundException">
        /// The key wasn't found.
        /// </exception>
        public void SeekWithKeyNotFoundException(string key)
        {
            if (!TrySeek(key))
            {
                throw new KeyNotFoundException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0} was not found", key));
            }
        }

        /// <summary>
        /// Position the cursor before the first record in the table.
        /// A <see cref="TryMoveNext"/> will then position the cursor
        /// on the first record.
        /// </summary>
        public void MoveBeforeFirst()
        {
            Api.MoveBeforeFirst(_sesid, _table);
        }

        /// <summary>
        /// Try to move to the next record.
        /// </summary>
        /// <returns>
        /// True if the move was successful, false if there are no more records.
        /// </returns>
        public bool TryMoveNext()
        {
            return Api.TryMoveNext(_sesid, _table);
        }

        /// <summary>
        /// Try to move to the previous record.
        /// </summary>
        /// <returns>
        /// True if the move was successful, false if there are no more records.
        /// </returns>
        public bool TryMovePrevious()
        {
            return Api.TryMovePrevious(_sesid, _table);
        }
         
        

        

        /// <summary>
        /// Delete the record the cursor is currently positioned on.
        /// </summary>
        public void DeleteCurrent()
        {
            Api.JetDelete(_sesid, _table);            
        }


        /// <summary>
        /// Calls JetMakeKey.
        /// </summary>
        /// <param name="key">The value of the key column.</param>
        internal void MakeKey(string key)
        {
            Api.MakeKey(_sesid, _table, key, Encoding.Unicode, MakeKeyGrbit.NewKey);            
        }
         
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Api.JetEndSession(_sesid, EndSessionGrbit.None);
            GC.SuppressFinalize(this);
        }

    }
}
