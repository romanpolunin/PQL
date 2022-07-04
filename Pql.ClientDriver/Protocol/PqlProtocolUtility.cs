using System.Data;

using Grpc.Core;

using Pql.ClientDriver.Protocol.Wire;

namespace Pql.ClientDriver.Protocol
{
    internal sealed class PqlProtocolUtility : IDisposable
    {
        public PqlDataConnection Connection;

        public AsyncDuplexStreamingCall<PqlRequestItem, PqlResponseItem> StreamingCall;
        
        private IDisposable[] _holders;

        /// <summary>
        /// Reads <see cref="DataResponse"/> from input stream referenced by <see cref="InputStream"/>.
        /// </summary>
        public Task<DataResponse> ReadResponse()
        {
            return ReadResponseHeaders(StreamingCall);
        }

        public static async Task<DataResponse> ReadResponseHeaders(AsyncDuplexStreamingCall<PqlRequestItem, PqlResponseItem> streamingCall)
        {
            var item = await streamingCall.ResponseStream.MoveNext() ? streamingCall.ResponseStream.Current : null;
            if (item is null || item.ItemCase != PqlResponseItem.ItemOneofCase.Header)
            {
                throw new DataException("Failed to retrieve response headers");
            }

            if (item.Header.ErrorCode != 0)
            {
                throw new DataException(string.Format(
                    "Server error code: {0}, message: {1}", item.Header.ErrorCode, item.Header.ServerMessage));
            }

            return item.Header;
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="connection">Parent connection</param>
        /// <param name="inputStream">Incoming data stream from server</param>
        /// <param name="holders">Array of holder objects that must be disposed when reader is closed</param>
        public PqlProtocolUtility(PqlDataConnection connection, AsyncDuplexStreamingCall<PqlRequestItem, PqlResponseItem> streamingCall)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
            StreamingCall = streamingCall ?? throw new ArgumentNullException(nameof(streamingCall));
        }

        /// <summary>
        /// Releases the managed resources used by the <see cref="T:System.Data.Common.DbDataReader"/> and optionally releases the unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            var conn = Interlocked.CompareExchange(ref Connection, null, Connection);
            var holders = Interlocked.CompareExchange(ref _holders, null, _holders);

            if (holders != null)
            {
                foreach (var holder in holders)
                {
                    if (holder != null)
                    {
                        holder.Dispose();
                    }
                }
            }

            if (conn != null)
            {
                conn.ConfirmExecutionCompletion(true);
            }
        }
    }
}