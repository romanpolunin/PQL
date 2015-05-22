using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pql.ClientDriver;
using Pql.Engine.DataContainer;

namespace Pql.Engine.UnitTest
{
    [TestClass]
    public class RamDriverTest
    {
        public static TestServiceContainer TestServiceContainer;

        [TestInitialize]
        public void InitializeEmbeddedServer()
        {
            if (TestServiceContainer == null)
            {
                TestServiceContainer = new TestServiceContainer();
                //AddSomeData(TestServiceContainer.StorageDriver);
                //TestServiceContainer.StorageDriver.WriteDescriptor(TestServiceContainer.StorageDriver.GetDescriptor());
                //TestServiceContainer.StorageDriver.FlushDataToStore();
            }
        }

        public void TestShutdown()
        {
            if (TestServiceContainer != null)
            {
                TestServiceContainer.Dispose();
                TestServiceContainer = null;
            }
        }

        [TestMethod]
        public void TestSelect()
        {
            TestSelect("select 1 from testdoc limit 1", -1);
            TestSelect("select 1 from testdoc", -1);
            TestSelect("select * from testdoc where fieldBool6", -1);
            TestSelect("select * from testdoc where id = 1 order by id", 1);
        }

        [TestMethod]
        public void TestParameters()
        {
            using (var conn = GetTestConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "select * from testdoc where @arg = 1 or SetContains(@ids, cast(id, 'int32'))";

                    var param = cmd.CreateParameter();
                    param.Direction = ParameterDirection.Input;
                    param.DbType = DbType.Int16;
                    param.Value = (Int16)1;
                    param.ParameterName = "@arg";
                    cmd.Parameters.Add(param);
                    
                    var paramIds = cmd.CreateParameter();
                    paramIds.Direction = ParameterDirection.Input;
                    paramIds.DbType = DbType.Int32;
                    paramIds.Value = new int[0];
                    paramIds.ParameterName = "@ids";
                    cmd.Parameters.Add(paramIds);
                    
                    var count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);
                    Assert.IsTrue(0 < count);

                    param.Value = (Int16)2;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);
                    Assert.AreEqual(0, count);

                    var data = new int[1000000];
                    for (var i = 0; i < data.Length; i++)
                    {
                        data[i] = i + 1; // ids in test data start with 1
                    }
                    paramIds.Value = data;

                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);
                }
            }

            using (var conn = GetTestConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "select * from testdoc where @arG is null";

                    var param = cmd.CreateParameter();
                    param.Direction = ParameterDirection.Input;
                    param.DbType = DbType.Int16;
                    param.Value = null;
                    param.ParameterName = "@aRg";
                    cmd.Parameters.Add(param);
                    
                    var count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);

                    param.Value = DBNull.Value;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);

                    cmd.CommandText = "select * from testdoc where id = @arg";
                    param.Value = (Int16)1;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);
                }
            }
            
            using (var conn = GetTestConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "select id from testdoc order by id limit @lIm offset @oFf";

                    var param = cmd.CreateParameter();
                    param.Direction = ParameterDirection.Input;
                    param.DbType = DbType.Int16;
                    param.Value = null;
                    param.ParameterName = "@arg";
                    cmd.Parameters.Add(param);

                    var limit = cmd.CreateParameter();
                    limit.Direction = ParameterDirection.Input;
                    limit.DbType = DbType.Int32;
                    limit.Value = 0;
                    limit.ParameterName = "@liM";
                    cmd.Parameters.Add(limit);
                    
                    var offset = cmd.CreateParameter();
                    offset.Direction = ParameterDirection.Input;
                    offset.DbType = DbType.Int32;
                    offset.Value = 0;
                    offset.ParameterName = "@OFF";
                    cmd.Parameters.Add(offset);
                    
                    var count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);

                    limit.Value = 1;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);

                    limit.Value = 2;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);

                    limit.Value = null;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);

                    offset.Value = 150;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);

                    offset.Value = 199;
                    count = cmd.ExecuteNonQuery();
                    Console.WriteLine("Count: " + count);
                }
            }
        }

        public void DeleteRange(int count, int firstId)
        {
            using (var conn = GetTestConnection())
            {
                using (var cmdDelete = (IPqlDbCommand)conn.CreateCommand())
                {
                    cmdDelete.CommandText = "delete from testdoc where id between " + firstId + " and " + (firstId + count - 1);

                    Console.WriteLine(cmdDelete.CommandText);

                    cmdDelete.CommandType = CommandType.Text;

                    var affected = cmdDelete.ExecuteNonQuery();
                    Console.WriteLine("Deleted: " + affected);
                    //Assert.AreEqual(count, affected);
                }
            }
        }

        [TestMethod]
        public void TestBig()
        {
            TestBigBulkInsertImpl(10000, 1000);
            TestBigBulkInsertImpl(1000000, 20000);
        }

        //private static ConcurrentBag<object> s_leakedConnectionsModel = new ConcurrentBag<object>();

        public void TestBigImpl(int count, int firstId)
        {
            using (var conn = GetTestConnection())
            //s_leakedConnectionsModel.Add(conn); // to prevent it from being disposed by GC

            using (var cmd = conn.CreateCommand())
            //var cmd = conn.CreateCommand();
            {
                //cmd.CommandText = "select * from testdoc where id between " + firstId + " and " + (firstId + count - 1) + " order by id";
                //cmd.CommandText = "select * from testdoc limit " + count + " offset " + (firstId - 1);
                cmd.CommandText = "select * from testdoc order by id limit @lim offset @off";

                cmd.Parameters.Add(
                    new PqlDataCommandParameter
                        {
                            DbType = DbType.Int32,
                            ParameterName = "@lim",
                            Value = count
                        });

                cmd.Parameters.Add(
                    new PqlDataCommandParameter
                        {
                            DbType = DbType.Int32,
                            ParameterName = "@off",
                            Value = firstId - 1
                        });

                var realcount = 0;

                cmd.ExecuteNonQuery();
                return;
                using (var reader = cmd.ExecuteReader())
                //var reader = cmd.ExecuteReader();
                {
                    var ordinal = reader.GetOrdinal("id");
                    while (reader.Read()) // && realcount < 900)
                    {
                        Assert.AreEqual(firstId + realcount, reader.GetInt64(ordinal));
                        realcount++;
                    }
                }

                //Assert.AreEqual(1, realcount);
                Assert.AreEqual(count, realcount);
            }
        }

        public void TestBigInsertImpl(int count, int firstId)
        {
            var dataArray = new byte[100];
            new Random().NextBytes(dataArray);

            var timer = Stopwatch.StartNew();
            
            using (var conn = GetTestConnection())
            {
                using (var cmdInsert = (IPqlDbCommand)conn.CreateCommand())
                {
                    cmdInsert.CommandText = 
                        "insert into testdoc(id,fieldbyte1,fieldguid2,fieldstring3,fieldbinary4,fielddecimal5,fieldbool6,fieldbool15,fielddecimal14) "
                    + "values (@id,@byte1,@guid2,@string3,@binary4,@decimal5,@bool6,@bool15,@decimal14)";

                    var id = AddParameter(cmdInsert, "@id", DbType.Int64);
                    var byte1 = AddParameter(cmdInsert, "@byte1", DbType.Byte);
                    var guid2 = AddParameter(cmdInsert, "@guid2", DbType.Guid);
                    var string3 = AddParameter(cmdInsert, "@string3", DbType.String);
                    var binary4 = AddParameter(cmdInsert, "@binary4", DbType.Binary);
                    var decimal5 = AddParameter(cmdInsert, "@decimal5", DbType.Currency);
                    var bool6 = AddParameter(cmdInsert, "@bool6", DbType.Boolean);
                    var bool12 = AddParameter(cmdInsert, "@bool15", DbType.Boolean);
                    var decimal11 = AddParameter(cmdInsert, "@decimal14", DbType.Currency);

                    for (Int64 idValue = firstId; idValue < firstId + count; idValue++)
                    {
                        id.Value = idValue;
                        byte1.Value = (Byte) idValue;
                        guid2.Value = Guid.NewGuid();
                        string3.Value = Environment.TickCount.ToString();
                        binary4.Value = dataArray;
                        decimal5.Value = (decimal)idValue;
                        bool6.Value = idValue % 2 == 0;
                        bool12.Value = idValue % 2 == 1;
                        decimal11.Value = (decimal)(idValue / 2.0);
                        
                        Assert.AreEqual(1, cmdInsert.ExecuteNonQuery());

                        if (idValue % 1000 == 0)
                        {
                            Console.WriteLine("Thread {0} progress: {1}", Thread.CurrentThread.ManagedThreadId, idValue - firstId);
                        }
                    }
                }
            }

            timer.Stop();
            Console.WriteLine(timer.ElapsedMilliseconds);
        }

        private static IDataParameter AddParameter(IDbCommand cmd, string name, DbType type)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.DbType = type;
            cmd.Parameters.Add(p);
            return p;
        }

        public void TestBigBulkInsertImpl(int count, int firstId)
        {
            var timer = Stopwatch.StartNew();
            
            using (var conn = GetTestConnection())
            {
                using (var cmdInsert = (IPqlDbCommand)conn.CreateCommand())
                {
                    var inserter = new DataGenBulk(count, firstId);

                    Assert.AreEqual(
                        count,
                        cmdInsert.BulkInsert("testdoc", inserter.FieldNames, inserter.Count,
                            inserter.GetInsertEnumerableForPerformanceTest()));
                }
            }

            timer.Stop();
            Console.WriteLine(timer.ElapsedMilliseconds);
        }

        public void TestDemoDataInsertImpl(int count, int firstId)
        {
            var timer = Stopwatch.StartNew();

            using (var conn = GetTestConnection())
            {
                using (var cmdInsert = (IPqlDbCommand)conn.CreateCommand())
                {
                    var inserter = new DataGenBulk(count, firstId);

                    Assert.AreEqual(
                        count,
                        cmdInsert.BulkInsert("testdoc", inserter.FieldNames, inserter.Count,
                            inserter.GetInsertEnumerableForDemoData()));
                }
            }

            timer.Stop();
            Console.WriteLine(timer.ElapsedMilliseconds);
        }

        public void Defragment()
        {
            var timer = Stopwatch.StartNew();
            
            using (var conn = GetTestConnection())
            {
                using (var cmd = (IPqlDbCommand)conn.CreateCommand())
                {
                    cmd.CommandText = "defragment";

                    Assert.AreEqual(0, cmd.ExecuteNonQuery());
                }
            }

            timer.Stop();
            Console.WriteLine(timer.ElapsedMilliseconds);
        }

        public void TestBigBulkUpdateImpl(int count, int firstId)
        {
            var timer = Stopwatch.StartNew();
            
            using (var conn = GetTestConnection())
            {
                using (var cmdInsert = (IPqlDbCommand)conn.CreateCommand())
                {
                    var inserter = new DataGenBulk(count, firstId);
                    inserter.UpdateMode = true;
                    Assert.AreEqual(count, cmdInsert.BulkUpdate("testdoc", inserter.FieldNames, inserter.Count, inserter.GetInsertEnumerableForPerformanceTest()));
                }
            }

            timer.Stop();
            Console.WriteLine(timer.ElapsedMilliseconds);
        }

        [TestMethod]
        public void TestBinaryValues()
        {
            Assert.AreEqual(1, ExecuteNonQuery("insert into testdoc(id, fieldbinary4) values(500, null)"));
            var data = GetScalarValue<byte[]>("select fieldbinary4 from testdoc where id=500");
            Assert.IsNull(data);
            Assert.AreEqual(1, ExecuteNonQuery("delete from testdoc where id=500"));

            Assert.AreEqual(1, ExecuteNonQuery("insert into testdoc(id, fieldbinary4) values(500, convert(null, 'binary'))"));
            data = GetScalarValue<byte[]>("select fieldbinary4 from testdoc where id=500");
            Assert.IsNull(data);
            Assert.AreEqual(1, ExecuteNonQuery("delete from testdoc where id=500"));

            var newdata = new byte[] {0, 1, 2, 255};
            var base64 = Convert.ToBase64String(newdata);
            Assert.AreEqual(1, ExecuteNonQuery("insert into testdoc(id, fieldbinary4) values(500, convert('" + base64 + "', 'binary'))"));
            data = GetScalarValue<byte[]>("select fieldbinary4 from testdoc where id=500");
            Assert.IsNotNull(data);
            Assert.IsTrue(data.SequenceEqual(newdata));
            Assert.AreEqual(1, ExecuteNonQuery("delete from testdoc where id=500"));
        }

        [TestMethod]
        public void TestSelectOrder()
        {
            using (var conn = GetTestConnection())
            {
                using (var command = conn.CreateCommand())
                {
                    var ascending = new List<long>();
                    var prev = long.MinValue;
                    foreach (var curr in GetEnumerable<long>(command, "select id from testdoc order by id"))
                    {
                        Assert.IsTrue(curr >= prev);
                        prev = curr;
                        ascending.Add(curr);
                    }

                    Assert.AreNotEqual(0, ascending.Count);

                    var descending = new List<long>();
                    prev = long.MaxValue;
                    foreach (var curr in GetEnumerable<long>(command, "select id from testdoc order by id desc"))
                    {
                        Assert.IsTrue(curr <= prev);
                        prev = curr;
                        descending.Add(curr);
                    }

                    Assert.AreNotEqual(0, descending.Count);

                    descending.Reverse();
                    Assert.IsTrue(ascending.SequenceEqual(descending));
                }
            }
        }

        [TestMethod]
        public void TestBasicSelect()
        {
            using (var conn = GetTestConnection())
            {
                var command = conn.CreateCommand();
                command.CommandText = "select * from testdoc";
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            Console.WriteLine("{0}, {1}, {2}", reader.GetName(i), reader.GetDataTypeName(i), reader.GetValue(i));
                        }
                        Console.WriteLine("---------------");
                    }
                }
            }
        }

        [TestMethod]
        public void TestBasicDelete()
        {
            var countBefore = GetEnumerable<object>("select 1 from testDoc").Count();
            Assert.AreEqual(2, ExecuteNonQuery("delete from testdoc where rownum() < 2"));
            var countAfter = GetEnumerable<int>("select 1 from testDoc").Count();
            Assert.AreEqual(countBefore - 2, countAfter);
        }

        [TestMethod]
        public void TestBasicUpdate()
        {
            Assert.AreEqual(1, ExecuteNonQuery("insert into testdoc(id, fieldbyte1) values(500, 24)"));
            var current = GetScalarValue<byte>("select fieldbyte1 from testDoc where id=500");
            Assert.AreEqual(24, current);

            Assert.AreEqual(1, ExecuteNonQuery("update testdoc set fieldbyte1=25 where id=500"));
            var modified = GetScalarValue<byte>("select fieldbyte1 from testDoc where id=500");
            Assert.AreEqual(25, modified);

            Assert.AreEqual(1, ExecuteNonQuery("delete from testdoc where id=500"));
        }

        void TestSelect(string commandText, int expectedCount)
        {
            var count = GetEnumerable<object>(commandText).Count();

            if (expectedCount == -1)
            {
                expectedCount = ExecuteNonQuery(commandText);
            }
            
            Assert.AreEqual(expectedCount, count);
        }

        private PqlDataConnection GetTestConnection()
        {
            SetThreadContext(-1);

            var conn = new PqlDataConnection();
            conn.ConnectionString = string.Format(
                "Server={0}/{1};Database={2}",
                TestServiceContainer.TestHostBaseAddressTcp,
                TestServiceContainer == null ? "default" : TestServiceContainer.ServiceInstanceName, 
                ConfigurationManager.AppSettings["PqlProcessorTestScopeId"]);

            conn.Open();
            return conn;
        }

        public int ExecuteNonQuery(string commandText)
        {
            using (var conn = GetTestConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    return ExecuteNonQuery(cmd, commandText);
                }
            }
        }

        private int ExecuteNonQuery(IDbCommand cmd, string commandText)
        {
            cmd.CommandText = commandText;
            return cmd.ExecuteNonQuery();
        }

        private T GetScalarValue<T>(string commandText)
        {
            using (var conn = GetTestConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = commandText;
                    return (T) cmd.ExecuteScalar();
                }
            }
        }

        public IEnumerable<T> GetEnumerable<T>(string commandText, int ordinal = 0)
        {
            using (var conn = GetTestConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    return GetEnumerable<T>(cmd, commandText, ordinal);
                }
            }
        }

        public IEnumerable<T> GetEnumerable<T>(IDbCommand cmd, string commandText, int ordinal = 0)
        {
            cmd.CommandText = commandText;
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    yield return (T) reader.GetValue(ordinal);
                }
            }
        }

        public static void SetThreadContext(int correlationId)
        {
            var userId = ConfigurationManager.AppSettings["PqlProcessorTestUserId"];
            var tenantId = ConfigurationManager.AppSettings["PqlProcessorTestTenantId"];
            PqlEngineSecurityContext.Set(new PqlClientSecurityContext(correlationId.ToString(), "test", tenantId, userId));
        }

        public void FlushDriverToStore()
        {
            var timer = Stopwatch.StartNew();
            TestServiceContainer.StorageDriver.FlushDataToStore();
            timer.Stop();
            Console.WriteLine("Written to disk in: " + timer.ElapsedMilliseconds + " ms");
        }

        public void ReadDataFromStore()
        {
            var timer = Stopwatch.StartNew();
            foreach (var doc in TestServiceContainer.StorageDriver.GetDescriptor().EnumerateDocumentTypes())
            {
                TestServiceContainer.StorageDriver.PrepareAllColumnsAndWait(doc.DocumentType);
            }
            timer.Stop();
            Console.WriteLine("Read from disk in: " + timer.ElapsedMilliseconds + " ms");
        }
    }
}
