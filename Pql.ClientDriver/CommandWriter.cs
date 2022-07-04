using System.Text;

using Grpc.Core;

using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Protocol.Wire;

namespace Pql.ClientDriver
{
    internal sealed class CommandWriter
    {
        private DataRequest _dataRequest;
        private DataRequestBulk _dataRequestBulk;
        private DataRequestParams _dataRequestParams;

        public void Attach(DataRequest dataRequest, DataRequestParams dataRequestParams, DataRequestBulk dataRequestBulk)
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

        public void Dispose()
        {

        }

        public void WriteTo(IAsyncStreamWriter<PqlRequestItem> stream)
        {
            if (_dataRequest == null)
            {
                throw new InvalidOperationException("Cannot write when not attached to a command");
            }

            // write headers
            stream.WriteAsync(new PqlRequestItem { Header = _dataRequest });

            // write parameters
            if (_dataRequestParams is not null)
            {
                stream.WriteAsync(new PqlRequestItem { Header = _dataRequest });

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
                /*
                BitVector.Write(notnulls, _dataRequestParams.Bulk.ParametersData.Count, bufferWriter);

                // write parameter data
                for (var ordinal = 0; ordinal < _dataRequestParams.Bulk.ParametersData.Count; ordinal++)
                {
                    if (BitVector.Get(notnulls, ordinal))
                    {
                        var param = _dataRequestParams.Bulk.ParametersData[ordinal];
                        param.Write(bufferWriter);
                    }

                    if (_stream.Length > 1000000)
                    {
                        BufferedReaderStream.WriteBlock(outputWriter, _stream);
                        _stream.SetLength(0);
                    }
                }

                if (_stream.Length > 0)
                {
                    BufferedReaderStream.WriteBlock(outputWriter, _stream);
                    _stream.SetLength(0);
                }

                BufferedReaderStream.WriteStreamEndMarker(outputWriter);
                */
            }

            // write data for bulk operation
            if (_dataRequestBulk is not null)
            {
                stream.WriteAsync(new PqlRequestItem { BulkHeader = _dataRequestBulk });
                /*
                foreach (var row in _dataRequestBulk.Bulk)
                {
                    row.Write(bufferWriter);

                    if (_stream.Length > 1000000)
                    {
                        BufferedReaderStream.WriteBlock(outputWriter, _stream);
                        _stream.SetLength(0);
                    }
                }

                if (_stream.Length > 0)
                {
                    BufferedReaderStream.WriteBlock(outputWriter, _stream);
                    _stream.SetLength(0);
                }

                BufferedReaderStream.WriteStreamEndMarker(outputWriter);*/
            }
        }
    }
}