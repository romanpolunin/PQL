using System.Diagnostics;

using Pql.UnmanagedLib;

namespace Pql.UnitTestProject
{
    public class TestBitVector
    {
        private static readonly Random s_rand = new Random(Environment.TickCount);
        private static readonly DynamicMemoryPool s_pool = new DynamicMemoryPool();

        public void TestRandomValuesSetter()
        {
            TestRandomValuesSetter(0);
            TestRandomValuesSetter(1);
            TestRandomValuesSetter(2);
            TestRandomValuesSetter(3);
            TestRandomValuesSetter(4);
            TestRandomValuesSetter(5);
            TestRandomValuesSetter(8);
            TestRandomValuesSetter(15);
            TestRandomValuesSetter(17);
            TestRandomValuesSetter(31);
            TestRandomValuesSetter(33);
            TestRandomValuesSetter(45_310_000);
            TestSetAll(100_000_000);
        }

        private void TestSetAll(int size)
        {
            var watch = Stopwatch.StartNew();
            using (var vector = new BitVector(s_pool))
            {
                vector.EnsureCapacity(size);
                for (ulong i = 0; i < vector.Capacity; i++)
                {
                    if (i % 3 == 0 || i % 7 == 0)
                    {
                        vector.Set(i);
                    }
                }

                for (ulong i = 0; i < vector.Capacity; i++)
                {
                    if (i % 3 == 0 || i % 7 == 0)
                    {
                        IsFalse(!vector.Get(i));
                    }
                }

                vector.ChangeAll(false);

                for (ulong i = 0; i < vector.Capacity; i++)
                {
                    IsFalse(vector.Get(i));
                }

                vector.ChangeAll(true);

                for (ulong i = 0; i < vector.Capacity; i++)
                {
                    IsFalse(!vector.Get(i));
                }
            }
            watch.Stop();
            Console.WriteLine("Elapsed {0} for size {1}", watch.ElapsedMilliseconds, size);
        }

        private void TestRandomValuesSetter(int size)
        {
            var watch = Stopwatch.StartNew();
            var data = CreateRandomBoolArray(size);

            using (var vector = new BitVector(s_pool))
            {
                vector.EnsureCapacity((ulong) size);

                // check both are false
                for (var i = 0; i < size; i++)
                {
                    IsFalse(vector.Get(i));
                }

                // assign new values and check state immediately after assignment
                for (var i = 0; i < data.Length; i++)
                {
                    //vector.EnsureCapacity((ulong)i + 1);

                    //if (data[i])
                    vector.Set(i);
                    //else
                    //    vector.SafeClear(i);

                    //if (i > 1)
                    //    AreEqual(data[i - 2], vector.Get(i - 2));
                    //if (i > 0)
                    //    AreEqual(data[i - 1], vector.Get(i - 1));
                    //AreEqual(data[i], vector.Get(i));
                }

                // check all are equal after all setters
                //for (var i = 0; i < data.Length; i++)
                //{
                //    AreEqual(data[i], vector.Get(i));
                //}

                watch.Stop();
                Console.WriteLine("Elapsed {0} for size {1}", watch.ElapsedMilliseconds, size);
            }
        }

        private void TestRandomValuesSetter2(ulong size, BitVector vector)
        {
            var watch = Stopwatch.StartNew();

            // assign new values and check state immediately after assignment
            for (ulong i = 0; i < size; i++)
            {
                if (i % 1000 == 0)
                {
                    vector.EnsureCapacity(i + 2000);
                }

                vector.Set(i);

                //IsFalse(!vector.Get(i));
            }

            watch.Stop();
            Console.WriteLine("Elapsed {0} for size {1}", watch.ElapsedMilliseconds, size);
        }

        private void MultiThread(Action a, int nThreads)
        {
            var tasks = new Task[nThreads];
            
            for (var i = 0; i < tasks.Length; i++)
            {
                tasks[i] = new Task(a);
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

        private void AreEqual(bool x, bool y)
        {
            if (x != y)
            {
                throw new Exception(string.Format("{0} != {1}", x, y));
            }
        }

        private void IsFalse(bool x)
        {
            if (x)
            {
                throw new Exception("Is true");
            }
        }

        public bool[] CreateRandomBoolArray(int size)
        {
            var result = new bool[size];
            for (var i = 0; i < size; i++)
            {
                result[i] = 0 == s_rand.Next(1000) % 2;
            }

            return result;
        }
    }
}