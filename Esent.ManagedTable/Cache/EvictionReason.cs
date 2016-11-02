using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Esent.ManagedTable
{
    public enum EvictionReason
    {
        None = 0,
        //
        // Summary:
        //     Manually
        Removed = 1,
        //
        // Summary:
        //     Overwritten
        Replaced = 2,
        //
        // Summary:
        //     Timed out
        Expired = 3,
        //
        // Summary:
        //     Event
        Dependency = 4,
        //
        // Summary:
        //     GC, overflow
        Capacity = 5
    }
}
