using System.Diagnostics;
using System.Runtime.InteropServices;

using Pql.UnmanagedLib;

namespace Pql.UnitTestProject
{
    internal class TestConcurrentHashmapOfKeys
    {
        private const Int64 PoolSize = ((Int64)3000) * 1000 * 1000;
        readonly IUnmanagedAllocator _pool = new DynamicMemoryPool();

        public unsafe void Test()
        {
            const ulong nThreads = 4;
            const ulong offset = 0;
            const ulong count = 1500000;

            for (var loop = 0; loop < 200; loop++)
            {
                using (var keys = GenerateKeys(nThreads * count))
                {
                    Console.WriteLine("Generated " + count * nThreads);
                    using (var map = new ConcurrentHashmapOfKeys(_pool))
                    {
                        MultiThread(InsertAndReadAction, map, keys, (int)nThreads, count, offset);
                        //MultiThread(ReadAction, map, keys, (int) nThreads, count, offset);
                        //Console.WriteLine("Inserted " + count * nThreads);
                    }

                    //TestPersistence(count, keys);

                    Console.WriteLine("Completed test");
                }

                //_pool.Recycle();
                //_pool.DeallocateGarbage();
                Console.WriteLine("Deallocated garbage");
            }

            Console.WriteLine("Disposed keys");
            //Console.ReadLine();
            Console.WriteLine("Disposed map");
            //Console.ReadLine();

            _pool.DeallocateGarbage();
            Console.WriteLine("Deallocated garbage. Press ENTER");
            Console.ReadLine();

            //_pool.Recycle();
            //Console.WriteLine("Recycled pool");
            //Console.ReadLine();
            _pool.Dispose();
            Console.WriteLine("Disposed pool. Press ENTER");
            Console.ReadLine();
        }

        private unsafe void BasicMemoryAction(ConcurrentHashmapOfKeys map, ExpandableArrayOfKeys keys, ulong first, ulong count, ulong offset)
        {
            for (var k = first; k < first + count; k++)
            {
                using (var map2 = new ConcurrentHashmapOfKeys(_pool))
                {
                    map2.TryAdd(keys.GetAt(0), 0);
                }
            }

            //var b = stackalloc void*[(int)100000];
            //for (var k = 0; k < 10000000000; k++)
            //{
            //    for (var i = 0; i < 100000; i++)
            //    {
            //        b[i] = _pool.Alloc((ulong) i % 10000);
            //    }
            //    for (var i = 0; i < 100000; i++)
            //    {
            //        _pool.Free(b[i]);
            //    }
            //}
        }

        private void TestPersistence(ulong count, ExpandableArrayOfKeys keys)
        {
            using (var validEntries = new BitVector(_pool))
            {
                validEntries.EnsureCapacity(count);
                validEntries.ChangeAll(true);

                using (var stream = new FileStream(@"c:\temp\data.d", FileMode.Create, FileAccess.ReadWrite))
                {
                    using (var writer = new BinaryWriter(stream))
                    {
                        keys.Write(writer, (ulong)count, validEntries);
                    }
                }

                using (var keys2 = new ExpandableArrayOfKeys(_pool))
                {
                    using (var stream = new FileStream(@"c:\temp\data.d", FileMode.Open, FileAccess.Read))
                    {
                        using (var reader = new BinaryReader(stream))
                        {
                            {
                                keys2.Read(reader, (ulong)count, validEntries);
                            }
                        }
                    }
                }
            }
        }

        private unsafe void InsertAndReadAction(ConcurrentHashmapOfKeys map, ExpandableArrayOfKeys keys, ulong first, ulong count, ulong offset)
        {
            var watch = Stopwatch.StartNew();

            for (var i = first; i < first + count; i++)
            {
                ulong val = 0;
                if (!map.TryAdd(keys.GetAt(i - offset), i))
                {
                    throw new Exception("Failed to insert " + i + ", offset from " + offset);
                }

                if (!map.TryGetValue(keys.GetAt(i - offset), ref val))
                {
                    throw new Exception("Failed to get at " + i + ", offset from " + offset);
                }

                if (val != i)
                {
                    throw new Exception("Failed to validate at " + i + ", offset from " + offset);
                }

                //Console.WriteLine("Added {0} at {1}", val, i);
            }

            watch.Stop();
            Console.WriteLine("Elapsed: {0}, for {1}, {2}", watch.ElapsedMilliseconds, first, count);
        }

        private unsafe void ReadAction(ConcurrentHashmapOfKeys map, ExpandableArrayOfKeys keys, ulong first, ulong count, ulong offset)
        {
            var watch = Stopwatch.StartNew();

            for (var k = 0; k < 10000; k++)
                for (var i = first; i < first + count; i++)
                {
                    ulong val = 0;
                    if (!map.TryGetValue(keys.GetAt(i - offset), ref val))
                    {
                        throw new Exception("Failed to get at " + i + ", offset from " + offset);
                    }

                    if (val != i)
                    {
                        throw new Exception("Failed to validate at " + i + ", offset from " + offset);
                    }

                    //Console.WriteLine("Added {0} at {1}", val, i);
                }

            watch.Stop();
            //Console.WriteLine("Elapsed: {0}, for {1}, {2}", watch.ElapsedMilliseconds, first, count);
        }

        private void MultiThread(
            Action<ConcurrentHashmapOfKeys, ExpandableArrayOfKeys, ulong, ulong, ulong> a,
            ConcurrentHashmapOfKeys map, ExpandableArrayOfKeys keys, int nThreads, ulong count, ulong offset)
        {
            var tasks = new Task[nThreads];

            var first = offset;

            for (var i = 0; i < tasks.Length; i++)
            {
                var first1 = first;
                tasks[i] = new Task(() => a(map, keys, first1, count, offset));
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

        unsafe ExpandableArrayOfKeys GenerateKeys(ulong count)
        {
            var result = new ExpandableArrayOfKeys(_pool);
            result.EnsureCapacity(count);

            var key = new byte[9];
            key[0] = 8;

            for (ulong i = 0; i < count; i++)
            {
                var bytes = BitConverter.GetBytes(i);
                Array.Copy(bytes, 0, key, 1, 8);

                //result[i-1] = key;
                if (!result.TrySetAt((int)i, key))
                {
                    throw new Exception("Failed to set a key element at " + (i));
                }
            }

            /*
            for (ulong i = 1; i <= count; i++)
            {
                var storedKey = result.GetAt(i - 1);

                var val = i;
                byte pos = 1;
                while (val != 0)
                {
                    key[pos++] = (byte)val;
                    val >>= 8;
                }

                key[0] = (byte)(pos - 1);

                if (storedKey[0] != key[0])
                {
                    throw new Exception("Length prefix broken at " + (i - 1));
                }

                for (var j = 0; j <= key[0]; j++)
                {
                    //Console.Write(storedKey[j]);
                    //Console.Write(',');

                    if (storedKey[j] != key[j])
                    {
                        throw new Exception("Data broken at " + (i - 1) + ", offset " + j);
                    }
                }

                //Console.WriteLine();
            }*/


            return result;
        }
    }
}