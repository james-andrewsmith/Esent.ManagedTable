using System;
using System.Diagnostics;
using Microsoft.Isam.Esent.Interop;

namespace EsentTempTableTest
{
    public class ManagedTableCursorCache<TConfig, TCursor> : IDisposable 
                                             where TConfig : ManagedTableConfig
                                             where TCursor : ManagedTableCursor<TConfig>
        
    {
        public ManagedTableCursorCache(Instance instance, 
                                       TConfig config,
                                       Func<Instance, TConfig, TCursor> openCursor)
        {
            _config = config;
            _instance = instance;            
            _cursors = new TCursor[MaxCachedCursors];
            _lockObject = new object();
            OpenCursor = openCursor;
        }

        private readonly Func<Instance, TConfig, TCursor> OpenCursor;

        /// <summary>
        /// The maximum number of cursors that can be cached.
        /// </summary>
        private const int MaxCachedCursors = 64;

        /// <summary>
        /// The underlying ESENT instance.
        /// </summary>
        private readonly Instance _instance;

        /// <summary>
        /// Configuration for the cursors.
        /// </summary>
        private readonly TConfig _config;

        /// <summary>
        /// The cached cursors.
        /// </summary>
        private readonly TCursor[] _cursors;

        /// <summary>
        /// Lock objects used to serialize access to the cursors.
        /// </summary>
        private readonly object _lockObject;



        /// <summary>
        /// Gets a new cursor. This will return a cached cursor if available,
        /// or create a new one.
        /// </summary>
        /// <returns>A new cursor.</returns>
        public TCursor GetCursor()
        {
            lock (_lockObject)
            {
                for (int i = 0; i < _cursors.Length; ++i)
                {
                    if (null != _cursors[i])
                    {
                        var cursor = _cursors[i];
                        _cursors[i] = null;
                        // Console.WriteLine($"Get Cursor {cursor._sesid.ToString()} via thread {System.Threading.Thread.CurrentThread.ManagedThreadId}");
                        return cursor;
                    }
                }
            }

            // Didn't find a cached cursor, open a new one
            var oc = OpenCursor(_instance, _config);
            // Console.WriteLine($"Open Cursor {oc._sesid.ToString()} via thread {System.Threading.Thread.CurrentThread.ManagedThreadId}");
            return oc;
        }

        /// <summary>
        /// Free a cursor. This will cache the cursor if the cache isn't full
        /// and dispose of it otherwise.
        /// </summary>
        /// <param name="cursor">The cursor to free.</param>
        public void FreeCursor(TCursor cursor)
        {
            Debug.Assert(null != cursor, "Freeing a null cursor");
            int transactionLevel = cursor.TransactionLevel;
            if (transactionLevel > 0)
            {
                throw new InvalidOperationException(
                    string.Format("Freeing a cursor with an outstanding transaction (level={0}).", transactionLevel));
            }

            lock (_lockObject)
            {
                for (int i = 0; i < _cursors.Length; ++i)
                {
                    if (null == _cursors[i])
                    {
                        _cursors[i] = cursor;
                        // Console.WriteLine($"Free Cursor {cursor._sesid.ToString()} via thread {System.Threading.Thread.CurrentThread.ManagedThreadId}");
                        return;
                    }
                }
            }

            // Didn't find a slot to cache the cursor in
            Console.WriteLine($"Dispose Cursor {cursor._sesid.ToString()} via thread {System.Threading.Thread.CurrentThread.ManagedThreadId}");
            cursor.Dispose();
        }    

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            for (int i = 0; i < _cursors.Length; ++i)
            {
                if (null != _cursors[i])
                {
                    _cursors[i].Dispose();
                    _cursors[i] = null;
                }
            }

            GC.SuppressFinalize(this);
        }
    }
}
