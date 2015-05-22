using System;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Pql.ClientDriver;
using Pql.Engine.DataContainer;

namespace Pql.Server.UnitTest
{
    class Program
    {
        private static string ConnectionString = "Server=localhost:5000/default;Initial Catalog=1:1";

        public static void Main(string[] args)
        {
            //new JsonCGI().RunCommand(args);
            //new TestClientDriver().Multithread();
            //new TestDataContainer().PopulateRedisWithDummyDriverData();
            ConsoleClient();
            //Test();
        }

        private static void Test(string command, IDbConnection conn, bool nonQuery)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = command;

                var cnt = 0;
                var timer = Stopwatch.StartNew();

                if (nonQuery)
                {
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Elapsed total: " + timer.ElapsedMilliseconds);
                }
                else
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        Console.WriteLine("Elapsed to open reader: " + timer.ElapsedMilliseconds);
                        try
                        {
                            while (reader.Read())
                            {
                                cnt++;
                                if (0 == cnt % 1000)
                                {
                                    Console.WriteLine(cnt);
                                    Console.WriteLine("Elapsed so far: " + timer.ElapsedMilliseconds);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.ToString());
                        }
                        finally
                        {
                            Console.WriteLine("Rows total: " + cnt);
                            Console.WriteLine("Elapsed total: " + timer.ElapsedMilliseconds);
                        }
                    }
                }
            }
        }

        private static void ConsoleClient()
        {
            var connectionString = ConfigurationManager.AppSettings["PqlProcessorTestUri"];
            if (!string.IsNullOrEmpty(connectionString))
            {
                var userId = ConfigurationManager.AppSettings["PqlProcessorTestUserId"];
                var tenantId = ConfigurationManager.AppSettings["PqlProcessorTestTenantId"];
                var scopeId = ConfigurationManager.AppSettings["PqlProcessorTestScopeId"];
                ConnectionString = string.Format(connectionString, scopeId);
                PqlEngineSecurityContext.Set(new PqlClientSecurityContext("1", "Beta3", tenantId, userId));
            }
            else
            {
                PqlEngineSecurityContext.Set(new PqlClientSecurityContext("1", "Beta3", "1", "1"));
            }
            
            while (true)
            {
                CommandPrompt();
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.DarkGray;
                var command = Console.ReadLine();
                Console.ForegroundColor = oldColor;
                if ("q".Equals(command))
                {
                    return;
                }

                if (!string.IsNullOrWhiteSpace(command))
                {
                    GetSomeData(command);
                }
            }
        }

        private static void CommandPrompt()
        {
            var oldColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.WriteLine();
            Console.WriteLine(" >********************************");
            Console.WriteLine(" >*******    q to exit    ********");
            Console.Write(ConnectionString);
            Console.WriteLine(" >");
            Console.ForegroundColor = oldColor;
        }

        static void GetSomeData(string commandText)
        {
            try
            {
                using (var conn = new PqlDataConnection())
                {
                    conn.ConnectionString = ConnectionString;
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = commandText;

                    var timer1 = Stopwatch.StartNew();
                    var timer2 = Stopwatch.StartNew();
                    var count = 0;

                    using (var reader = cmd.ExecuteReader())
                    {
                        timer1.Stop();

                        var formatString = CreateFormatString(reader);

                        var normalColor = Console.ForegroundColor;
                        var nullValueColor = ConsoleColor.DarkGray;

                        while (reader.Read())
                        {
                            count++;

                            Console.WriteLine("---------------------");
                            for (var ordinal = 0; ordinal < reader.FieldCount; ordinal++)
                            {
                                Console.Write(formatString, ordinal, reader.GetName(ordinal), reader.GetFieldType(ordinal).Name);
                                
                                if (reader.IsDBNull(ordinal))
                                {
                                    Console.ForegroundColor = nullValueColor;
                                    Console.WriteLine("<null>");
                                    Console.ForegroundColor = normalColor;
                                }
                                else
                                {
                                    Console.WriteLine(reader.GetValue(ordinal));
                                }

                                if (Console.KeyAvailable)
                                {
                                    Console.ReadKey(true);
                                    throw new Exception("Key pressed, dataset traversal aborted");
                                }
                            }
                        }

                        timer2.Stop();
                    }

                    Console.WriteLine("---------------------");
                    Console.WriteLine("{0} records, {1} ms to initiate, {2} ms to complete",
                        count, timer1.ElapsedMilliseconds, timer2.ElapsedMilliseconds);
                }
            }
            catch (Exception e)
            {
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e.ToString());
                Console.ForegroundColor = oldColor;
            }
        }

        private static string CreateFormatString(IDataReader reader)
        {
            int maxFieldNameLen = 0;
            int maxDbTypeNameLen = 0;
            for (var i = 0; i < reader.FieldCount; i++)
            {
                maxFieldNameLen = Math.Max(maxFieldNameLen, reader.GetName(i).Length);
                maxDbTypeNameLen = Math.Max(maxDbTypeNameLen, reader.GetDataTypeName(i).Length);
            }
            
            return string.Format("{{0,3}} - {{1,{0}}} - {{2,{1}}}: ", maxFieldNameLen, maxDbTypeNameLen);
        }
    }
}
