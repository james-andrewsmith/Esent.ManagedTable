using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Esent.ManagedTable
{
    public class CacheEntryOptions
    {

        private DateTimeOffset? _absoluteExpiration;
        private TimeSpan? _absoluteExpirationRelativeToNow;
        private TimeSpan? _slidingExpiration;

        /// <summary>
        /// Gets or sets an absolute expiration date for the cache entry.
        /// </summary>
        public DateTimeOffset? AbsoluteExpiration
        {
            get
            {
                return _absoluteExpiration;
            }
            set
            {
                _absoluteExpiration = value;
            }
        }

        /// <summary>
        /// Gets or sets an absolute expiration time, relative to now.
        /// </summary>
        public TimeSpan? AbsoluteExpirationRelativeToNow
        {
            get
            {
                return _absoluteExpirationRelativeToNow;
            }
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(AbsoluteExpirationRelativeToNow),
                        value,
                        "The relative expiration value must be positive.");
                }

                _absoluteExpirationRelativeToNow = value;
            }
        }

        /// <summary>
        /// Gets or sets how long a cache entry can be inactive (e.g. not accessed) before it will be removed.
        /// This will not extend the entry lifetime beyond the absolute expiration (if set).
        /// </summary>
        public TimeSpan? SlidingExpiration
        {
            get
            {
                return _slidingExpiration;
            }
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(SlidingExpiration),
                        value,
                        "The sliding expiration value must be positive."
                    );
                }
                _slidingExpiration = value;
            }
        }

        /// <summary>
        /// Gets the <see cref="IChangeToken"/> instances which cause the cache entry to expire.
        /// </summary>
        public IList<string> Dependencies { get; } = new List<string>();

        /// <summary>
        /// Gets or sets the callbacks will be fired after the cache entry is evicted from the cache.
        /// </summary>
        public IList<CacheEntryEvictionDelegate> PostEvictionCallbacks { get; } = new List<CacheEntryEvictionDelegate>();
         
    }
}
