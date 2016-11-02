using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Esent.ManagedTable
{
    /// <summary>
    /// Signature of the callback which gets called when a cache entry expires.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="reason">The <see cref="EvictionReason"/>.</param>
    /// <param name="state">The information that was passed when registering the callback.</param>
    public delegate void CacheEntryEvictionDelegate(object key, object value, EvictionReason reason);
}
