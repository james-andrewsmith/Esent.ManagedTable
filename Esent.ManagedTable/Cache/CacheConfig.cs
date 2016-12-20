using System;
using Microsoft.Database.Isam.Config;

namespace Esent.ManagedTable
{
    public class CacheConfig : ManagedTableConfig
    {
        public CacheConfig(int size)
        {
            _size = size;
        }

        private readonly int _size;

        public override string Database
        {
            get;
            internal set;
        }

        public override string TableName
        {
            get { return "cache"; }
        }

        public override DatabaseConfig GetDefaultDatabaseConfig()
        {
            var currentProcess = System.Diagnostics.Process.GetCurrentProcess();

            return new DatabaseConfig()
            {
                // Global params
                CacheSizeMin = (_size / 64) * 8192,
                CacheSize = (_size / 64) * 8192,
                EnableFileCache = true,
                DatabaseFilename = $"{currentProcess.ProcessName}-{currentProcess.Id}.edb"
            };
        }

        public string KeyColumnName
        {
            get { return "key"; }
        }

        public string DependencyColumnName
        {
            get { return "dependency"; }
        }

        public string DataColumnName
        {
            get { return "data"; }
        }

        public string CallbacksColumnName
        {
            get { return "callback"; }
        }

        public string LastAccessedColumnName
        {
            get { return "lastaccessed"; }
        }

        public string SlidingExpirationColumnName
        {
            get { return "sliding_expiration"; }
        }

        public string AbsoluteExpirationColumnName
        {
            get { return "absolute_expiration"; }
        }

        public string PrimaryKeyIndexName
        {
            get { return "idx_primary"; }
        }

        public string DependencyIndexName
        {
            get { return "idx_dependency"; }
        }

        public string SlidingExpiryIndexName
        {
            get { return "idx_sliding"; }
        }

        public string AbsoluteExpiryIndexName
        {
            get { return "idx_absolute"; }
        }


        
    }
}
