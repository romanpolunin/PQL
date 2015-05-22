using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Pql.UnitTestProject
{
    internal class TestConcurrentDictOfKeys 
    {
        public unsafe void Test()
        {
            Action<ConcurrentDictionary<ulong, ulong>, ulong, ulong> action = (map, first, count) =>
                {
                    var watch = Stopwatch.StartNew();

                    for (var i = first; i < first + count; i++)
                    {
                        if (!map.TryAdd(i, i))
                        {
                            throw new Exception("Failed to insert " + i);
                        }

                        ulong val;
                        if (!map.TryGetValue(i, out val))
                        {
                            throw new Exception("Failed to validate at " + i);
                        }

                        //Console.WriteLine("Added {0} at {1}", val, i);
                    }

                    watch.Stop();
                    Console.WriteLine("Elapsed: {0}, for {1}, {2}", watch.ElapsedMilliseconds, first, count);
                };

            {
                var dict = new ConcurrentDictionary<ulong, ulong>();
                ulong nThreads = 4;
                ulong count = 1000000;
                MultiThread(action, dict, (int)nThreads, count);
                for (ulong i = 1; i < 1 + nThreads * count; i++)
                {
                    ulong val;
                    if (!dict.TryGetValue(i, out val))
                    {
                        throw new Exception(string.Format("Failed to get at {0}", i));
                    }

                    if (val != i)
                    {
                        throw new Exception(string.Format("Failed at {0} with {1}", i, val));
                    }
                }

                dict = null;
                GC.Collect(2, GCCollectionMode.Forced);
                GC.Collect(2, GCCollectionMode.Forced);
                GC.Collect(2, GCCollectionMode.Forced);
                GC.Collect(2, GCCollectionMode.Forced);

                Console.WriteLine("Completed test");
                Console.ReadLine();
            }
        }

        private void MultiThread(Action<ConcurrentDictionary<ulong, ulong>, ulong, ulong> a, ConcurrentDictionary<ulong, ulong> map, int nThreads, ulong count)
        {
            var tasks = new Task[nThreads];

            ulong first = 1;

            for (var i = 0; i < tasks.Length; i++)
            {
                var first1 = first;
                tasks[i] = new Task(() => a(map, first1, count));
                first += count;
            }

            foreach (var task in tasks)
            {
                task.Start();
            }

            foreach (var task in tasks)
            {
                task.Wait();
            }
        }
    }
}