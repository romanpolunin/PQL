using Grpc.Core;

namespace Pql.Server
{
    public class PqlService : Pql.Server.Protocol.Wire.PqlService.PqlServiceBase
    {
        public override Task Request(IAsyncStreamReader<Protocol.Wire.PqlRequestItem> requestStream, IServerStreamWriter<Protocol.Wire.PqlResponseItem> responseStream, ServerCallContext context)
        {
            return Task.CompletedTask;
        }
    }
}