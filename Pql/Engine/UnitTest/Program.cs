using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Pql.Engine.UnitTest
{
    class Program
    {
        //static void Main(string[] args)
        //{
        //    var driver = new RamDriver();
        //    driver.Initialize(new RamDriverSettings {StorageRoot = @"C:\temp\ramdriver"});
        //    driver.ReadDataFromStore();
        //}

        static void Main(string[] args)
        {
            //TaskScheduler.UnobservedTaskException += (EventHandler<UnobservedTaskExceptionEventArgs>) ((sender, e) =>
            //    {
            //        if (e.Observed)
            //            return;
            //        e.SetObserved();
            //    });

            //GenerateDemoData();
            ReadDemoData();
            //PerformanceTest();
        }

        private static void ReadDemoData()
        {
            var test = new RamDriverTest();
            try
            {
                test.InitializeEmbeddedServer();
                test.ReadDataFromStore();
            }
            finally
            {
                test.TestShutdown();
            }
        }

        private static void GenerateDemoData()
        {
            var test = new RamDriverTest();
            try
            {
                test.InitializeEmbeddedServer();

                // warm-up
                RunMultithreaded(test, 8, 1, 100000, DemoDataGenAction);

                test.FlushDriverToStore();
            }
            finally
            {
                test.TestShutdown();
            }
        }

        private static void PerformanceTest()
        {
            var test = new RamDriverTest();
            try
            {
                test.InitializeEmbeddedServer();

                // warm-up
                RunMultithreaded(test, 8, 1, 10, TestThreadAction);

                Console.WriteLine("Press ENTER to start");
                Console.ReadLine();

                // now go
                RamDriverTest.SetThreadContext(-1);

                var count = 100000;
                var numThreads = 8;

                for (var i = 0; i < 5; i++)
                {
                    test.DeleteRange(100000000, -1);
                    RunMultithreaded(test, numThreads, 1, count, TestThreadAction);
                    ShowCount(test);
                }

                test.DeleteRange(1000, 1000);
                ShowCount(test);
            }
            finally
            {
                test.TestShutdown();
            }

            Console.WriteLine("Press ENTER to run full GC");
            Console.ReadLine();
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.Collect(2, GCCollectionMode.Forced, true);
            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static void RunMultithreaded(
            RamDriverTest test, int numThreads, int firstId, int countPerInterval,
            Action<RamDriverTest, int, object> threadAction)
        {
            Action<object> action = fid =>
                {
                    RamDriverTest.SetThreadContext((int)fid);

                    for (var i = 0; i < 1; i++)
                    {
                        threadAction(test, countPerInterval, fid); 
                    }
                };

            var threads = new Task[numThreads];

            for (var i = 0; i < threads.Length; i++)
            {
                threads[i] = new Task(action, firstId + i * countPerInterval, TaskCreationOptions.LongRunning);
            }

            var timer = Stopwatch.StartNew();

            for (var i = 0; i < threads.Length; i++)
            {
                threads[i].Start();
            }

            for (var i = 0; i < threads.Length; i++)
            {
                try
                {
                    threads[i].Wait();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }

            timer.Stop();
            Console.WriteLine("Elapsed ms: {0,3}, rps: {1,3}", timer.ElapsedMilliseconds, countPerInterval * threads.Length * 1000.0 / timer.ElapsedMilliseconds);

            //RamDriverTest.TestServiceContainer.StorageDriver.WriteDescriptor(RamDriverTest.TestServiceContainer.StorageDriver.GetDescriptor());
            //RamDriverTest.TestServiceContainer.StorageDriver.FlushDataToStore();
        }

        private static void DemoDataGenAction(RamDriverTest test, int countPerInterval, object fid)
        {
            test.TestDemoDataInsertImpl(countPerInterval, (int) fid);
        }

        private static void TestThreadAction(RamDriverTest test, int countPerInterval, object fid)
        {
            //test.TestBigImpl(countPerInterval, (int)fid);
            //test.TestBigBulkInsertImpl(countPerInterval, (int)fid);
            //test.DeleteRange(countPerInterval, (int)fid);
            test.TestBigBulkInsertImpl(countPerInterval, (int)fid);
            //test.TestBigInsertImpl(countPerInterval, (int)fid); 
            //test.TestBigBulkUpdateImpl(countPerInterval, (int)fid);
            //test.DeleteRange(countPerInterval, (int)fid);
            //test.TestBigBulkInsertImpl(countPerInterval, (int)fid);
        }

        private static void ShowCount(RamDriverTest test)
        {
            var realCount = test.ExecuteNonQuery("select 1 from testdoc");//" order by id");
            Console.WriteLine("Current number of records in the container: " + realCount);
        }
    }
}
