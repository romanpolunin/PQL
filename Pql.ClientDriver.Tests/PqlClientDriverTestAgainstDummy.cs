using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Pql.ClientDriver.Tests
{
    [TestClass]
    public class PqlClientDriverTestAgainstDummy : IntegrationTestBase<PqlDummyService>
    {
        public PqlClientDriverTestAgainstDummy()
            : base(new())
        {
        }


        [TestMethod]
        public async Task BasicHeaderExchange()
        {
            var client = PqlDataConnection.CreateClient(Channel);

            using var call = client.Request();
            await call.RequestStream.WriteAsync(new Protocol.Wire.PqlRequestItem
            {
                Header = new Protocol.Wire.DataRequest { CommandText = "ping" }
            });

            await call.RequestStream.CompleteAsync();

            Assert.IsTrue(await call.ResponseStream.MoveNext(System.Threading.CancellationToken.None));
            var response = call.ResponseStream.Current;
            Assert.AreEqual(0, response.Header.ErrorCode);
            Assert.AreEqual("pong", response.Header.ServerMessage);
        }
    }
}