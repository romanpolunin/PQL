using System;
using System.IO;
using System.Text;
using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Wcf;
using ProtoBuf;
using ProtoBuf.Meta;

namespace Pql.ClientDriver
{
    internal sealed class CommandWriter : IPqlDataWriter
    {
        private readonly MemoryStream m_stream = new MemoryStream(100000);

        private DataRequest m_dataRequest;
        private DataRequestBulk m_dataRequestBulk;
        private DataRequestParams m_dataRequestParams;

        public void Attach(DataRequest dataRequest, DataRequestParams dataRequestParams, DataRequestBulk dataRequestBulk)
        {
            if (dataRequest == null)
            {
                throw new ArgumentNullException("dataRequest");
            }

            if (dataRequestBulk == null && string.IsNullOrEmpty(dataRequest.CommandText))
            {
                throw new InvalidOperationException("Command text is empty");
            }

            m_dataRequest = dataRequest;
            m_dataRequestParams = dataRequestParams;
            m_dataRequestBulk = dataRequestBulk;
        }

        public void Dispose()
        {
            
        }

        public void WriteTo(Stream stream)
        {
            if (m_dataRequest == null)
            {
                throw new InvalidOperationException("Cannot write when not attached to a command");
            }

            // write headers
            m_dataRequest.HaveRequestBulk = m_dataRequestBulk != null;
            m_dataRequest.HaveParameters = m_dataRequestParams != null && m_dataRequestParams.DataTypes.Length > 0;
            Serializer.SerializeWithLengthPrefix(stream, m_dataRequest, PrefixStyle.Base128);

            // write parameters
            if (m_dataRequest.HaveParameters)
            {
                Serializer.SerializeWithLengthPrefix(stream, m_dataRequestParams, PrefixStyle.Base128);

                m_stream.SetLength(0);

                using (var bufferWriter = new BinaryWriter(m_stream, Encoding.UTF8, true))
                using (var outputWriter = new BinaryWriter(stream, Encoding.UTF8, true))
                {
                    // write notnull flags
                    var notnulls = new int[BitVector.GetArrayLength(m_dataRequestParams.Bulk.ParametersData.Count)];
                    for (var i = 0; i < m_dataRequestParams.Bulk.ParametersData.Count; i++)
                    {
                        var param = m_dataRequestParams.Bulk.ParametersData[i];
                        if (param.Value != null && param.Value != DBNull.Value)
                        {
                            BitVector.Set(notnulls, i);
                        }
                    }

                    BitVector.Write(notnulls, m_dataRequestParams.Bulk.ParametersData.Count, bufferWriter);

                    // write parameter data
                    for (var ordinal = 0; ordinal < m_dataRequestParams.Bulk.ParametersData.Count; ordinal++)
                    {
                        if (BitVector.Get(notnulls, ordinal))
                        {
                            var param = m_dataRequestParams.Bulk.ParametersData[ordinal];
                            param.Write(bufferWriter);
                        }

                        if (m_stream.Length > 1000000)
                        {
                            BufferedReaderStream.WriteBlock(outputWriter, m_stream);
                            m_stream.SetLength(0);
                        }
                    }

                    if (m_stream.Length > 0)
                    {
                        BufferedReaderStream.WriteBlock(outputWriter, m_stream);
                        m_stream.SetLength(0);
                    }

                    BufferedReaderStream.WriteStreamEndMarker(outputWriter);
                }
            }
            
            // write data for bulk operation
            if (m_dataRequest.HaveRequestBulk)
            {
                Serializer.SerializeWithLengthPrefix(stream, m_dataRequestBulk, PrefixStyle.Base128);

                m_stream.SetLength(0);
                
                using (var bufferWriter = new BinaryWriter(m_stream, Encoding.UTF8, true))
                using (var outputWriter = new BinaryWriter(stream, Encoding.UTF8, true))
                {
                    foreach (var row in m_dataRequestBulk.Bulk)
                    {
                        row.Write(bufferWriter);

                        if (m_stream.Length > 1000000)
                        {
                            BufferedReaderStream.WriteBlock(outputWriter, m_stream);
                            m_stream.SetLength(0);
                        }
                    }

                    if (m_stream.Length > 0)
                    {
                        BufferedReaderStream.WriteBlock(outputWriter, m_stream);
                        m_stream.SetLength(0);
                    }

                    BufferedReaderStream.WriteStreamEndMarker(outputWriter);
                }
            }
        }
    }
}