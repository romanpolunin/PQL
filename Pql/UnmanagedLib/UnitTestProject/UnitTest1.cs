using System;
using System.Threading.Tasks;
using Pql.UnmanagedLib;

namespace Pql.UnitTestProject
{
    public class UnitTest1
    {
        public unsafe void TestMethod1()
        {
            //var segmentSize = 1000000U;
            var chunkSize = 1000U;
            //var pool = new FixedMemoryPool(segmentSize);
            var pool = new DynamicMemoryPool();

            Action test = () =>
                {
                    for (var i = 0; i < 1000000; i++)
                    {
                        if (i%100000 == 0)
                        {
                            Console.Write("Step " + i);
                            Console.Write(" : ");
                        }
                        //pool.Free(pool.Alloc(chunkSize));
                        pool.Alloc(chunkSize);
                    }
                };

            var threads = new Task[12];
            for (var i = 0; i < threads.Length; i++)
            {
                threads[i] = new Task(test, TaskCreationOptions.LongRunning);
            }

            foreach (var task in threads)
            {
                task.Start();
            }

            foreach (var task in threads)
            {
                task.Wait();
            }

            Console.ReadLine();
        }
    }
}
