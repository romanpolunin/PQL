using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Grpc.Core;

using Pql.Server.Protocol.Wire;

namespace Pql.ClientDriver.Tests
{
    public class PqlDummyService : PqlService.PqlServiceBase
    {
        public override async Task Request(IAsyncStreamReader<PqlRequestItem> requestStream, IServerStreamWriter<PqlResponseItem> responseStream, ServerCallContext context)
        {
            PqlRequestItem item;
            if (!await requestStream.MoveNext())
            {
                return;
            }

            item = requestStream.Current;

            if (item.Header?.CommandText == "ping")
            {
                await responseStream.WriteAsync(new PqlResponseItem
                {
                    Header = new DataResponse { ErrorCode = 0 }
                });

                return;
            }
        }
    }
}