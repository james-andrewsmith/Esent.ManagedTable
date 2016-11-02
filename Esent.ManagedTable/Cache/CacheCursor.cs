using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;

namespace Esent.ManagedTable
{
    public class CacheCursor : ManagedTableCursor<CacheConfig>
    {
        public CacheCursor(Instance instance, CacheConfig config) : base(instance, config)
        {
            // Get the IDs of the columns specific to this implementation 
            _keyColumn = Api.GetTableColumnid(_sesid, _table, _config.KeyColumnName);
            _dataColumn = Api.GetTableColumnid(_sesid, _table, _config.DataColumnName);
            _dependencyColumn = Api.GetTableColumnid(_sesid, _table, _config.DependencyColumnName);
            _callbacksColumn = Api.GetTableColumnid(_sesid, _table, _config.CallbacksColumnName);
            _lastAccessedColumn = Api.GetTableColumnid(_sesid, _table, _config.LastAccessedColumnName);
            _slidingExpirationColumn = Api.GetTableColumnid(_sesid, _table, _config.SlidingExpirationColumnName);
            _absoluteExpirationColumn = Api.GetTableColumnid(_sesid, _table, _config.AbsoluteExpirationColumnName);
        }


        private readonly JET_COLUMNID _keyColumn;        
        private readonly JET_COLUMNID _dataColumn;
        private readonly JET_COLUMNID _dependencyColumn;
        private readonly JET_COLUMNID _callbacksColumn;
        private readonly JET_COLUMNID _lastAccessedColumn;
        private readonly JET_COLUMNID _slidingExpirationColumn;
        private readonly JET_COLUMNID _absoluteExpirationColumn;

        

        /// <summary>
        /// Replace the value column of the record the cursor is currently on.
        /// </summary>
        /// <param name="value">The new value.</param>
        public void Replace(CacheEntry entry)
        {
            SetEntry(entry, JET_prep.Replace);
             


        }

        /// <summary>
        /// Insert data into the data table. No record with the same key
        /// should exist.
        /// </summary>
        /// <param name="data">The data to add.</param>
        public void Insert(CacheEntry entry)
        {
            SetEntry(entry, JET_prep.Insert);
        }

        private void SetEntry(CacheEntry entry, JET_prep updateType)
        {
            Api.JetPrepareUpdate(_sesid, _table, updateType);
            try
            {
                // The easy part set the key
                Api.SetColumn(
                    _sesid,
                    _table,
                    _keyColumn,
                    entry.Key,
                    Encoding.Unicode
                );

                Api.SetColumn(
                    _sesid,
                    _table,
                    _dataColumn,
                    entry.Data
                );

                Api.SetColumn(
                    _sesid,
                    _table,
                    _callbacksColumn,
                    entry.PostEvicationCallbacks
                );

                // Avoid race conditions on update by making sure the record
                // is clear 
                if (updateType == JET_prep.Replace)
                {
                    // Get the existing channels 
                    var column = new JET_RETRIEVECOLUMN
                    {
                        columnid = _dependencyColumn,
                        grbit = RetrieveColumnGrbit.RetrieveTag
                    };
                    Api.JetRetrieveColumns(_sesid, _table, new[] { column }, 1);

                    // This is a bit tricky, note that when an item is removed all the 
                    // itag sequences update to be one lower
                    int count = column.itagSequence; 
                    for (int i = 0; i < count; i++)
                    {
                        JET_SETINFO setInfo = new JET_SETINFO { itagSequence = 1 };
                        Api.JetSetColumn(
                            _sesid,
                            _table,
                            _dependencyColumn,
                            null,
                            0,
                            SetColumnGrbit.UniqueMultiValues,
                            setInfo
                        );
                    }

                    Api.SetColumn(_sesid, _table, _absoluteExpirationColumn, null);
                    Api.SetColumn(_sesid, _table, _lastAccessedColumn, null);
                    Api.SetColumn(_sesid, _table, _slidingExpirationColumn, null);
                }


                if (entry.AbsoluteExpiration > 0)
                {                   
                    Api.SetColumn(
                        _sesid,
                        _table,
                        _absoluteExpirationColumn,
                        entry.AbsoluteExpiration
                    );
                }
                else if (entry.SlidingExpiration > 0)
                {
                    Api.SetColumn(
                        _sesid,
                        _table,
                        _lastAccessedColumn,
                        Convert.ToInt64(entry.LastAccessed)
                    );

                    Api.SetColumn(
                        _sesid,
                        _table,
                        _slidingExpirationColumn,
                        entry.SlidingExpiration
                    );
                }

                // Loop through all the channels and create them,
                // as this is a new record there is no need for checks 
                foreach (var dependency in entry.Dependencies)
                {
                    // Using tag sequence 0 wil mean the items are 
                    // automatically pushed into the mv-column
                    JET_SETINFO setInfo = new JET_SETINFO();
                    setInfo.itagSequence = 0;

                    // Note the use of ASCII to take up half the space
                    // there is no reason either subscriber or channel
                    // will have more than this
                    byte[] data = Encoding.ASCII.GetBytes(dependency);
                    Api.JetSetColumn(
                        _sesid,
                        _table,
                        _dependencyColumn,
                        data,
                        data.Length,
                        SetColumnGrbit.UniqueMultiValues,
                        setInfo
                    );
                }

                Api.JetUpdate(_sesid, _table);
            }
            catch (Exception exp)
            {
                Api.JetPrepareUpdate(_sesid, _table, JET_prep.Cancel);
                throw exp;
            }
        }

        public byte[] GetData()
        {            
            return Api.RetrieveColumn(_sesid, _table, _dataColumn);
        }

        public long GetLastAccess()
        {            
            var last = Api.RetrieveColumnAsInt64(_sesid, _table, _lastAccessedColumn);
            if (last.HasValue) return last.Value;
            return 0;
        }

        public void SetLastAccess(long epoch)
        {
            Api.JetPrepareUpdate(_sesid, _table, JET_prep.Replace);
            try
            {
                Api.SetColumn(
                    _sesid,
                    _table,
                    _lastAccessedColumn,
                    epoch
                );
                Api.JetUpdate(_sesid, _table);
            }
            catch (Exception exp)
            {
                Api.JetPrepareUpdate(_sesid, _table, JET_prep.Cancel);
                throw exp;
            }
        }

        /// <summary>
        /// Gets the full entry object with all of its properties populated
        /// </summary>
        /// <returns></returns>
        public CacheEntry GetEntry()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Only gets the properties need to fire post eviction callback delegates, 
        /// this is optimised as evictions can have a cascading effect on entries.
        /// </summary>
        /// <returns></returns>
        public CacheEntry GetEntryForCallback()
        {
            var entry = new CacheEntry();
            entry.Key = Api.RetrieveColumnAsString(_sesid, _table, _keyColumn, Encoding.Unicode);
            entry.Data = Api.RetrieveColumn(_sesid, _table, _dataColumn);
            entry.PostEvicationCallbacks = Api.RetrieveColumn(_sesid, _table, _callbacksColumn);

            /*
            var absolute = Api.RetrieveColumnAsInt64(_sesid, _table, _absoluteExpirationColumn);
            if (absolute.HasValue)
            {
                entry.AbsoluteExpiration = absolute.Value;
            }
            else
            {
                var sliding = Api.RetrieveColumnAsInt32(_sesid, _table, _slidingExpirationColumn);
                if (sliding.HasValue)
                {
                    entry.SlidingExpiration = sliding.Value;
                    entry.LastAccessed = Api.RetrieveColumnAs
                }
            }


            // Get the existing channels 
            var column = new JET_RETRIEVECOLUMN
            {
                columnid = _dependencyColumn,
                grbit = RetrieveColumnGrbit.RetrieveTag
            };

            Api.JetRetrieveColumns(_sesid, _table, new[] { column }, 1);            

            int count = column.itagSequence;
            entry.Dependencies = new string[count];
            for (int i = 1; i <= count; i++)
            {
                JET_RETINFO retinfo = new JET_RETINFO { itagSequence = i };
                byte[] data = Api.RetrieveColumn(_sesid, _table, _dependencyColumn, RetrieveColumnGrbit.None, retinfo);
                entry.Dependencies[i] = Encoding.ASCII.GetString(data);                
            }
            */

            

            return entry;
        }

        internal IEnumerable<CacheEntry> RemoveByDependency(string dependency)
        {
            Api.JetSetCurrentIndex(_sesid, _table, _config.DependencyIndexName);
            Api.MakeKey(_sesid, _table, dependency, Encoding.ASCII, MakeKeyGrbit.NewKey);

            var list = new List<CacheEntry>();
            if (Api.TrySeek(_sesid, _table, SeekGrbit.SeekEQ))
            {
                Api.MakeKey(_sesid, _table, dependency, Encoding.ASCII, MakeKeyGrbit.NewKey);
                Api.JetSetIndexRange(_sesid, _table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

                if (Api.TrySetIndexRange(_sesid, _table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit))
                {
                    do
                    {
                        list.Add(GetEntryForCallback());
                    }
                    while (Api.TryMoveNext(_sesid, _table));
                }
            }

            Api.JetSetCurrentIndex(_sesid, _table, null);
            return list;
        }

        internal IEnumerable<CacheEntry> RemoveByAbsoluteExpiryLessThan(long epoch)
        {
            Api.JetSetCurrentIndex(_sesid, _table, _config.AbsoluteExpiryIndexName);
            Api.MakeKey(_sesid, _table, 1L, MakeKeyGrbit.NewKey);

            var list = new List<CacheEntry>();
            if (Api.TrySeek(_sesid, _table, SeekGrbit.SeekGE))
            {
                Api.MakeKey(_sesid, _table, epoch, MakeKeyGrbit.NewKey);

                if (Api.TrySetIndexRange(_sesid, _table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit))
                {
                    // the index range has been created and we are on the first record
                    do
                    {
                        list.Add(GetEntryForCallback());                        
                    }
                    while (Api.TryMoveNext(_sesid, _table));
                }

            }

            Api.JetSetCurrentIndex(_sesid, _table, null);
            return list;
        }

        internal IEnumerable<CacheEntry> RemoveBySlidingExpiry(long epoch)
        {
            Api.JetSetCurrentIndex(_sesid, _table, _config.SlidingExpiryIndexName);

            Api.MakeKey(_sesid, _table, 1L, MakeKeyGrbit.NewKey);
            Api.MakeKey(_sesid, _table, 1, MakeKeyGrbit.None);
            var list = new List<CacheEntry>();
            if (Api.TrySeek(_sesid, _table, SeekGrbit.SeekGE))
            {
                do
                {
                    var accessedValue = Api.RetrieveColumnAsInt64(_sesid, _table, _lastAccessedColumn, RetrieveColumnGrbit.RetrieveFromIndex);
                    var slidingValue = Api.RetrieveColumnAsInt32(_sesid, _table, _slidingExpirationColumn, RetrieveColumnGrbit.RetrieveFromIndex);

                    if (accessedValue.HasValue && 
                        slidingValue.HasValue)
                    {
                        if (accessedValue.Value + slidingValue.Value < epoch)
                        {
                            list.Add(GetEntryForCallback());
                        }
                    }                   
                }
                while (Api.TryMoveNext(_sesid, _table)) ;
                
            }

            Api.JetSetCurrentIndex(_sesid, _table, null);
            return list;
        }
    }
}
