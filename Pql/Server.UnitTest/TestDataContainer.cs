using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Pql.Engine.DataContainer.RamDriver;
using Pql.Engine.Interfaces.Internal;
using Pql.IntegrationStubs;

namespace Pql.Server.UnitTest
{
    [TestClass]
    public class TestDataContainer
    {
        [TestMethod]
        public void TestSchemaBuilder()
        {
            var builder = new SchemaBuilder();
            builder.AddDocumentTypeNames("Filter");

            builder.BeginDefineDocumentTypes();

            builder.AddDocumentTypeDescriptor(
                "Filter", "Filter",
                "Id", DbType.Int64,
                "Name", DbType.String,
                "QueryString", DbType.String,
                "IsDefault", DbType.Boolean,
                "IsNamed", DbType.Boolean,
                "Type", DbType.Int32
                );

            builder.AddIdentifierAliases(
                     "Filter",
                     "Filter.Id", "id",
                     "Filter.Name", "name",
                     "Filter.IsDefault", "isdefault");
            var desc = builder.Commit();
            var driver = new RamDriver();
            driver.Initialize(new DummyTracer(), new RamDriverSettings {Descriptor = desc});
        }
    }
}