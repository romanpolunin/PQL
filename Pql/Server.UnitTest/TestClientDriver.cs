using System;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pql.ClientDriver;

namespace Pql.Server.UnitTest
{
    [TestClass]
    public class TestClientDriver
    {
        private static long s_globalCounter = 0;
        private static Stopwatch s_globalTimer;
        private static readonly int NumClients = Environment.ProcessorCount;

        [TestMethod]
        public void Multithread()
        {
            Thread.Sleep(2000);

            var threads = new Thread[NumClients];

            for (var i = 0; i < threads.Length; i++)
            {
                threads[i] = new Thread(TestDummyData);
            }

            s_globalTimer = Stopwatch.StartNew();

            for (var i = 0; i < threads.Length; i++)
            {
                threads[i].Start();
            }

            for (var i = 0; i < threads.Length; i++)
            {
                try
                {
                    threads[i].Join();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }

        [TestMethod]
        public void TestDummyData()
        {
            try
            {
                using (var conn = new PqlDataConnection())
                {
                    conn.ConnectionString = "Server=localhost:5000/default;Initial Catalog=1:1";
                    var cmd = conn.CreateCommand();
                    for (var i = 0; i < 100000; i++)
                    {
                        {
                            //cmd.CommandText = "select field1,field2,field3,field4,field5,field6,field7,field8,field9,field10,field11,field12,field13,field14,field15,field16,field17,field18,field19,field20 from testDoc where field3+2=3 and field4 in ('1',field5) order by field6 desc";
                            cmd.CommandText = "select * from testDoc";
                            ConsumePregeneratedData(cmd, false);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static void ConsumePregeneratedData(IDbCommand cmd, bool validate)
        {
            using (var reader = cmd.ExecuteReader())
            {
                //for (var i = 0; i < reader.FieldCount; i++)
                //{
                //    Console.WriteLine("{0} / {1} / {2}", reader.GetName(i), reader.GetDataTypeName(i), reader.GetOrdinal(reader.GetName(i)));
                //}
                //Console.WriteLine("Receiving data on thread " + Thread.CurrentThread.ManagedThreadId);

                long localCounter = 0;
                long newCounter;
                int fieldCount = reader.FieldCount;
                var theGuid = Guid.Parse("C288273F-953D-4BB7-9753-69B81C567577");
                var bytes = new byte[35];
                var chars = new char[35];
                while (reader.Read())
                {
                    var isnull = false;
                    for (var indexInResponse = 0; indexInResponse < fieldCount; indexInResponse++)
                    {
                        if (isnull != reader.IsDBNull(indexInResponse))
                        {
                            throw new Exception("Expected IsDbBull to be " + isnull);
                        }

                        if (validate)
                        {
                            bool isGood;

                            if (0 == indexInResponse % 5)
                            {
                                isGood = reader.GetByte(indexInResponse) == (isnull ? 0 : (byte) indexInResponse);
                            }
                            else if (1 == indexInResponse % 5)
                            {
                                isGood = reader.GetGuid(indexInResponse).Equals(isnull ? Guid.Empty : theGuid);
                            }
                            else if (2 == indexInResponse % 5)
                            {
                                var specimen = "mystring" + indexInResponse;
                                isGood = reader.GetString(indexInResponse) == (isnull ? null : specimen);

                                if (isGood)
                                {
                                    isGood = 3 == reader.GetChars(indexInResponse, 0, chars, 0, 3)
                                             && chars[0] == 'm' && chars[1] == 'y' && chars[2] == 's';

                                    if (isGood)
                                    {
                                        var number = indexInResponse.ToString();
                                        isGood = number.Length == reader.GetChars(indexInResponse, 8, chars, 0, number.Length)
                                                 && number == new string(chars, 0, number.Length);
                                    }
                                }
                            }
                            else if (3 == indexInResponse % 5)
                            {
                                isGood = reader.GetBytes(indexInResponse, 0, bytes, 0, 35) == (isnull ? 0 : 30);
                                if (isGood && !isnull)
                                {
                                    for (var k = 0; k < 30; k++)
                                    {
                                        if (bytes[k] != (byte) indexInResponse)
                                        {
                                            isGood = false;
                                            break;
                                        }
                                    }
                                }
                            }
                            else if (4 == indexInResponse % 5)
                            {
                                isGood = reader.GetDecimal(indexInResponse) == (isnull ? 0 : indexInResponse);
                            }
                            else
                            {
                                throw new Exception("Fail");
                            }

                            if (!isGood)
                            {
                                throw new Exception("Bad value");
                            }

                        }

                        //isnull = !isnull;
                    }

                    localCounter++;

                    if (localCounter == 100000)
                    {
                        newCounter = Interlocked.Add(ref s_globalCounter, localCounter);
                        localCounter = 0;
                        Console.WriteLine("Elapsed ms: {0:N0}, counter: {1:N0}", s_globalTimer.ElapsedMilliseconds, newCounter);
                    }
                }

                newCounter = Interlocked.Add(ref s_globalCounter, localCounter);
                Console.WriteLine("Elapsed ms: {0:N0}, counter: {1:N0}", s_globalTimer.ElapsedMilliseconds, newCounter);
                
                Interlocked.Add(ref s_globalCounter, localCounter);
            }
        }
    }
}
