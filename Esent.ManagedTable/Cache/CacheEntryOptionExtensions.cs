using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Esent.ManagedTable
{
    public static class CacheEntryExtensions
    { 

        /// <summary>
        /// Expire the cache entry if the given <see cref="IChangeToken"/> expires.
        /// </summary>
        /// <param name="options">The <see cref="MemoryCacheEntryOptions"/>.</param>
        /// <param name="expirationToken">The <see cref="IChangeToken"/> that causes the cache entry to expire.</param>
        public static CacheEntryOptions AddDependency(this CacheEntryOptions options, string dependency)
        {
            if (string.IsNullOrEmpty(dependency))
            {
                throw new ArgumentNullException(nameof(dependency));
            }

            if (!options.Dependencies.Contains(dependency))
                options.Dependencies.Add(dependency);

            return options;
        }

        /// <summary>
        /// Sets an absolute expiration time, relative to now.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="relative"></param>
        public static CacheEntryOptions SetAbsoluteExpiration(this CacheEntryOptions options, TimeSpan relative)
        {
            options.AbsoluteExpirationRelativeToNow = relative;
            return options;
        }

        /// <summary>
        /// Sets an absolute expiration date for the cache entry.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="absolute"></param>
        public static CacheEntryOptions SetAbsoluteExpiration(this CacheEntryOptions options, DateTimeOffset absolute)
        {
            options.AbsoluteExpiration = absolute;
            return options;
        }

        /// <summary>
        /// Sets how long the cache entry can be inactive (e.g. not accessed) before it will be removed.
        /// This will not extend the entry lifetime beyond the absolute expiration (if set).
        /// </summary>
        /// <param name="options"></param>
        /// <param name="offset"></param>
        public static CacheEntryOptions SetSlidingExpiration(this CacheEntryOptions options, TimeSpan offset)
        {
            options.SlidingExpiration = offset;
            return options;
        }

        /// <summary>
        /// The given callback will be fired after the cache entry is evicted from the cache.
        /// </summary>
        /// <param name="options"></param>
        /// <param name="callback"></param>
        public static CacheEntryOptions RegisterPostEvictionCallback(this CacheEntryOptions options, CacheEntryEvictionDelegate callback)
        {
            if (callback == null)
            {
                throw new ArgumentNullException(nameof(callback));
            }

            options.PostEvictionCallbacks.Add(callback);

            return options;
        }
         
    }
}
