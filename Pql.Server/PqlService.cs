using Grpc.Core;

using Pql.Server.Protocol.Wire;

namespace Pql.Server
{
    public class PqlService : Protocol.Wire.PqlService.PqlServiceBase
    {
        public override Task Request(
            IAsyncStreamReader<PqlRequestItem> requestStream, 
            IServerStreamWriter<PqlResponseItem> responseStream, 
            ServerCallContext context) => Task.CompletedTask;
    }
}