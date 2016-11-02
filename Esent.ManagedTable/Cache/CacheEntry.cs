using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Esent.ManagedTable
{
    public class CacheEntry
    {
        public string Key
        {
            get;
            set;
        }

        public byte[] Data
        {
            get;
            set;
        }

        // Perhaps part of data?
        public byte[] PostEvicationCallbacks
        {
            get;
            set;
        }
        
        public string[] Dependencies
        {
            get;
            set;
        }

        public long LastAccessed
        {
            get;
            set;
        }

        public int SlidingExpiration
        {
            get;
            set;
        }

        public long AbsoluteExpiration
        {
            get;
            set;
        }

        internal EvictionReason _EvictionReason
        {
            get;
            set;
        }


    }
}
