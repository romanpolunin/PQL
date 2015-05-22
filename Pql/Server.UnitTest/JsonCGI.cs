using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Pql.ClientDriver;
using Pql.Engine.DataContainer;

namespace Pql.Server.UnitTest
{
    public class JsonCGI
    {
        private static string ConnectionString = "Server=localhost:5000/default;Initial Catalog=1:1";
        private const string UsageText = "Usage: \r\n\tPql.DataServices.PqlProcessor.Test.exe q|u|prepare \"QUERY TEXT\"";

        public void RunCommand(params string[] commandline)
        {
            var data = GetData(commandline);

            var serializer = JsonSerializer.Create(new JsonSerializerSettings
                {
                    
                });
            
            using (var stream = new MemoryStream(1000000))
            {
                using (var writer = new StreamWriter(stream))
                {
                    serializer.Serialize(writer, data);
                    writer.Flush();
                    stream.Seek(0, SeekOrigin.Begin);

                    stream.WriteTo(Console.OpenStandardOutput());
                }
            }
        }

        public CGIResponseData GetData(string[] commandline)
        {
            if (commandline == null || commandline.Length != 2)
            {
                return new CGIResponseData("Error in command line. " + UsageText);
            }

            try
            {
                InitThreadContext();

                var commandtype = commandline[0].ToLowerInvariant();
                var commandtext = commandline[1];

                using (var connection = new PqlDataConnection())
                {
                    connection.ConnectionString = ConnectionString;
                    connection.Open();
                    var cmd = connection.CreateCommand();
                    cmd.CommandText = commandtext;

                    if (commandtype == "prepare")
                    {
                        cmd.Prepare();
                        return new CGIResponseData("Prepared.");
                    }

                    if (commandtype == "u")
                    {
                        var raff = cmd.ExecuteNonQuery();
                        return new CGIResponseData("Update complete. Affected records: " + raff);
                    }
                    
                    if (commandtype == "q")
                    {
                        using (var reader = cmd.ExecuteReader())
                        {
                            return new CGIResponseData(reader);
                        }
                    }

                    return new CGIResponseData("Invalid command type: " + commandtype + ". " + UsageText);
                }
            }
            catch (Exception e)
            {
                return new CGIResponseData(e.Message);
            }
        }
        
        private void InitThreadContext()
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
        }
    }

    [DataContract]
    public class CGIResponseData
    {
        [DataMember]
        public string Message;
        [DataMember]
        public string[] Captions;
        [DataMember]
        public string[] DataTypes;
        [DataMember]
        public string[][] Values;

        public CGIResponseData(string message)
        {
            Message = message;
        }

        public CGIResponseData(IDataReader reader)
        {
            Captions = new string[reader.FieldCount];
            DataTypes = new string[reader.FieldCount];

            for (var ordinal = 0; ordinal < Captions.Length; ordinal++)
            {
                Captions[ordinal] = reader.GetName(ordinal);
                DataTypes[ordinal] = reader.GetDataTypeName(ordinal);
            }

            var rows = new List<string[]>(1000);
            while (reader.Read())
            {
                var row = new string[Captions.Length];
                for (var ordinal = 0; ordinal < Captions.Length; ordinal++)
                {
                    if (reader.IsDBNull(ordinal))
                    {
                        row[ordinal] = null;
                    }
                    else
                    {
                        var value = reader.GetValue(ordinal);
                        row[ordinal] = value == null ? null : value.ToString();
                    }
                }

                rows.Add(row);
            }

            Values = rows.ToArray();
        }
    }
}
