using System.Collections.Concurrent;
using System.Diagnostics;

namespace Pql.UnitTestProject
{
    internal class TestConcurrentDictOfKeys
    {
        public unsafe void Test()
        {
            Action<ConcurrentDictionary<ulong, ulong>, ulong[], ulong, ulong> action = (map, keys, first, count) =>
                {
                    var watch = Stopwatch.StartNew();

                    for (var i = first; i < first + count; i++)
                    {
                        if (!map.TryAdd(keys[i], i))
                        {
                            throw new Exception("Failed to insert " + i);
                        }

                        ulong val;
                        if (!map.TryGetValue(keys[i], out val))
                        {
                            throw new Exception("Failed to validate at " + i);
                        }

                        //Console.WriteLine("Added {0} at {1}", val, i);
                    }

                    watch.Stop();
                    Console.WriteLine("Elapsed: {0}, for {1}, {2}", watch.ElapsedMilliseconds, first, count);
                };

            {
                ulong nThreads = 4;
                ulong count = 60_000_000;
                var dict = new ConcurrentDictionary<ulong, ulong>();
                var keys = new ulong[count];
                var rand = new Random();
                for (ulong i = 0; i < count; i++)
                {
                    keys[i] = i;
                }

                for (var k = 0; k < 20; k++)
                {
                    dict.Clear();
                    MultiThread(action, dict, keys, (int)nThreads, count);
                    for (ulong i = 0; i < count; i++)
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
                }

                dict = null;
                GC.Collect(2, GCCollectionMode.Forced);
                GC.Collect(2, GCCollectionMode.Forced);
                GC.Collect(2, GCCollectionMode.Forced);
                GC.Collect(2, GCCollectionMode.Forced);

                Console.WriteLine("Completed test. Press [ENTER]");
                Console.ReadLine();
            }
        }

        private void MultiThread(Action<ConcurrentDictionary<ulong, ulong>, ulong[], ulong, ulong> a, ConcurrentDictionary<ulong, ulong> map, ulong[] keys, int nThreads, ulong count)
        {
            var tasks = new Task[nThreads];

            ulong first = 0;

            for (var i = 0; i < tasks.Length; i++)
            {
                var first1 = first;
                tasks[i] = new Task(() => a(map, keys, first1, count/(ulong)nThreads));
                first += count/(ulong)nThreads;
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