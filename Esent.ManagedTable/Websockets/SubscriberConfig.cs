using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Database.Isam.Config;

namespace Esent.ManagedTable
{
    public class SubscriberConfig : ManagedTableConfig
    {
        public override string Database
        {
            get;
            internal set;
        }

        public override string TableName
        {
            get { return "websocket"; }
        }

        public override DatabaseConfig GetDefaultDatabaseConfig()
        {
            return null;
        }


        public string SubscriberColumnName
        {
            get { return "subscriber"; }
        }

        public string ChannelColumnName
        {
            get { return "channel"; }
        }
    }
}
