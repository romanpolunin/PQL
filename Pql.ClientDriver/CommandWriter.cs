using System.Text;

using Google.Protobuf;

using Grpc.Core;

using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Protocol.Wire;

namespace Pql.ClientDriver
{
    internal sealed class CommandWriter
    {
        private readonly DataRequest _dataRequest;
        private readonly DataRequestParams _dataRequestParams;
        private readonly DataRequestBulk? _dataRequestBulk;

        public CommandWriter(DataRequest dataRequest, DataRequestParams dataRequestParams, DataRequestBulk? dataRequestBulk)
        {
            if (dataRequest == null)
            {
                throw new ArgumentNullException(nameof(dataRequest));
            }

            if (dataRequestBulk == null && string.IsNullOrEmpty(dataRequest.CommandText))
            {
                throw new InvalidOperationException("Command text is empty");
            }

            _dataRequest = dataRequest;
            _dataRequestParams = dataRequestParams;
            _dataRequestBulk = dataRequestBulk;
        }

        public async Task WriteTo(IAsyncStreamWriter<PqlRequestItem> stream)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            using var buffer = new MemoryStream();
            using var bufferWriter = new BinaryWriter(buffer);
            
            // write headers
            await stream.WriteAsync(new PqlRequestItem { Header = _dataRequest });

            // write parameters
            if (_dataRequestParams is not null)
            {
                await stream.WriteAsync(new PqlRequestItem { Header = _dataRequest });

                // write notnull flags
                var notnulls = new int[BitVector.GetArrayLength(_dataRequestParams.Bulk.ParametersData.Count)];
                for (var i = 0; i < _dataRequestParams.Bulk.ParametersData.Count; i++)
                {
                    var param = _dataRequestParams.Bulk.ParametersData[i];
                    if (param.Value != null && param.Value != DBNull.Value)
                    {
                        BitVector.Set(notnulls, i);
                    }
                }

                BitVector.Write(notnulls, _dataRequestParams.Bulk.ParametersData.Count, bufferWriter);

                // write parameter data
                for (var ordinal = 0; ordinal < _dataRequestParams.Bulk.ParametersData.Count; ordinal++)
                {
                    if (BitVector.Get(notnulls, ordinal))
                    {
                        var param = _dataRequestParams.Bulk.ParametersData[ordinal];
                        param.Write(bufferWriter);
                        bufferWriter.Flush();

                        var data = UnsafeByteOperations.UnsafeWrap(buffer.ToArray().AsMemory());
                        buffer.SetLength(0);
                        await stream.WriteAsync(new PqlRequestItem
                        {
                            ParamsRow = data
                        });

                        buffer.SetLength(0);
                    }
                }
            }

            // write data for bulk operation
            if (_dataRequestBulk is not null)
            {
                await stream.WriteAsync(new PqlRequestItem { BulkHeader = _dataRequestBulk });
                
                foreach (var row in _dataRequestBulk.Bulk)
                {
                    row.Write(bufferWriter);
                    bufferWriter.Flush();
                    var data = UnsafeByteOperations.UnsafeWrap(buffer.ToArray().AsMemory());
                    buffer.SetLength(0);

                    await stream.WriteAsync(new PqlRequestItem
                    {
                        BulkRow = data
                    });
                }
            }
        }
    }
}