using System;
using Microsoft.Database.Isam.Config;

namespace EsentTempTableTest
{
    public abstract class ManagedTableConfig
    {
        public abstract string Database
        {
            get;
            internal set;
        }

        public abstract string TableName
        {
            get;
        }

        public abstract DatabaseConfig GetDefaultDatabaseConfig();
        // public abstract Func<Instance, ManagedTableConfig, ManagedTableCursor<ManagedTableConfig>> OpenCursor();

    }
}
