using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Lz4Net;


namespace Esent.ManagedTable
{
#if DEBUG
    class Program
    {
        static void Main(string[] args)
        {
            // todo: 
            // In production make one of these a runtime variable
            // so there is no overlap if multiple processes are running            
                  
            /*
            using (var cache = new SubscriberTable())
            {
                cache.Subscribe("sub1", new[] { "chan1", "chan2" });
                cache.Subscribe("sub1", new[] { "chanx", "chan2" });
                cache.Unsubscribe("sub1", "chan2");
                var sub1Chans = cache.GetSubscriberChannels("sub1").ToList();

                cache.Subscribe("sub2", new[] { "chan1", "chan3" });
                cache.Subscribe("sub3", new[] { "chan1", "chan4" });

                cache.UnsubscribeAll("sub2");

                var chanXSubs = cache.GetSubscribers("chanx").ToList();
                var chan1Subs = cache.GetSubscribers("chan1").ToList();
                var chan2Subs = cache.GetSubscribers("chan2").ToList();
                var chan3Subs = cache.GetSubscribers("chan3").ToList();
                var chan4Subs = cache.GetSubscribers("chan4").ToList();
                
                Console.ReadLine();
            }*/
            
            using (var cache = new CacheTable(256))
            {

                // Test the ram usage

                // Add 12,000 88kb sample pages 
                var bytes = System.IO.File.ReadAllBytes("Cache\\cache-sample.html");
                var sw = System.Diagnostics.Stopwatch.StartNew();
                for (int i = 0; i < 12000; i++)
                {
                    var test = Lz4.CompressBytes(bytes, 0, bytes.Length, Lz4Mode.Fast);

                    // Todo:
                    // Make sure they have at least 3 depdenencies
                    cache.Set(
                        $"hash:test:{i}",
                        test,
                        new CacheEntryOptions()
                            // .SetSlidingExpiration(TimeSpan.FromSeconds(5))
                            .RegisterPostEvictionCallback(OnPostEvication)
                    );
                }

                Console.WriteLine($"Writing 12k records took {sw.Elapsed.TotalSeconds}");
                sw.Restart();

                for (int i = 0; i < 12000; i++)
                {
                    var data = cache.GetData(
                        $"hash:test:{i}"
                    );

                    var test = Lz4.DecompressBytes(data);
                }

                Console.WriteLine($"Reading 12k records took {sw.Elapsed.TotalSeconds}");
                sw.Restart();


                // Access the first 1000 a few times to cache them
                for (int k = 0; k < 12; k++)
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        var data = cache.GetData(
                            $"hash:test:{i}"
                        );

                        var test = Lz4.DecompressBytes(data);
                    }
                }

                Console.WriteLine($"Reading 1k hot records 12 times took {sw.Elapsed.TotalMilliseconds}");
                sw.Restart();
                Console.ReadLine();
                
                cache.Set(
                    $"hash:test:slide5",
                    new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 },
                    new CacheEntryOptions()
                        .SetSlidingExpiration(TimeSpan.FromSeconds(5))
                        .RegisterPostEvictionCallback(OnPostEvication)
                );

                cache.Set(
                    $"hash:test:slide10",
                    new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 },
                    new CacheEntryOptions()
                        .SetSlidingExpiration(TimeSpan.FromSeconds(10))
                        .RegisterPostEvictionCallback(OnPostEvication)
                );

                cache.Set(
                    $"hash:test:slide60",
                    new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 },
                    new CacheEntryOptions()
                        .SetSlidingExpiration(TimeSpan.FromSeconds(60))
                        .RegisterPostEvictionCallback(OnPostEvication)
                );

                cache.Set(
                    $"hash:test",
                    new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 },
                    new CacheEntryOptions()
                        .SetAbsoluteExpiration(TimeSpan.FromSeconds(5))
                        .RegisterPostEvictionCallback(OnPostEvication)
                );

                
                cache.Set(
                    $"hash:test1",
                    new byte[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                    new CacheEntryOptions()
                        .SetAbsoluteExpiration(TimeSpan.FromSeconds(10))
                        .RegisterPostEvictionCallback(OnPostEvication)
                );

                
                cache.Set(
                    $"hash:test2",
                    new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 },
                    new CacheEntryOptions()
                        .SetAbsoluteExpiration(TimeSpan.FromSeconds(60))
                        .RegisterPostEvictionCallback(OnAnotherPostEvication)
                );

                cache.Set(
                    $"hash:test3",
                    new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 },
                    new CacheEntryOptions()
                        .RegisterPostEvictionCallback(OnAnotherPostEvication)
                );

                System.Threading.Thread.Sleep(TimeSpan.FromSeconds(10));
                cache.StartScanForExpiredItems(true);
                Console.ReadLine();


                // test the cleanup, should onlu remove one


                // cache.GetData()                     

                cache.RemoveByDependency("dep:0001");

                // RemoveByKey
                // RemoveByDependency
                // GetByKey

                // Cursor
                // -> GetPostEvictionDelegate
                // -> 

                // Remove all the test keys which remain 
                for(int i = 0; i < 12000; i++)
                    cache.RemoveByKey($"{i}");

                // Dump the whole cache without callbacks
                cache.Clear();
            }
           
        }


        private static void OnPostEvication(object key, object value, EvictionReason reason)
        {
            Console.WriteLine($"{key} evicted for {reason}");
        }

        private static void OnAnotherPostEvication(object key, object value, EvictionReason reason)
        {
            Console.WriteLine($"{key} evicted for {reason} using second delegate");
        }
    }
#endif
}
