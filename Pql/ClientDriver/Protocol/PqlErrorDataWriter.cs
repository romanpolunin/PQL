using System;
using System.IO;
using Pql.ClientDriver.Wcf;
using ProtoBuf;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Utility class, used to write error information into output stream
    /// </summary>
    public sealed class PqlErrorDataWriter : IPqlDataWriter
    {
        private readonly Exception m_exception;
        private readonly int m_errorCode;
        private readonly bool m_writeBlockHeaderAndStreamEnd;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="errorCode">Optional error code</param>
        /// <param name="exception">Optional exception information</param>
        /// <param name="writeBlockHeaderAndStreamEnd">True to write a block header</param>
        public PqlErrorDataWriter(int errorCode, Exception exception, bool writeBlockHeaderAndStreamEnd)
        {
            m_errorCode = errorCode;
            m_exception = exception;
            m_writeBlockHeaderAndStreamEnd = writeBlockHeaderAndStreamEnd;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            
        }

        /// <summary>
        /// Writes data to output stream.
        /// </summary>
        public void WriteTo(Stream stream)
        {
            var dataResponse = new DataResponse(m_errorCode, m_exception == null ? "Unknown error" : m_exception.Message);

            if (m_writeBlockHeaderAndStreamEnd)
            {
                using (var buffer = new MemoryStream())
                {
                    Serializer.SerializeWithLengthPrefix(buffer, dataResponse, PrefixStyle.Base128);

                    var blockSize = checked((Int32) buffer.Length);
                    stream.Write(RowData.BlockHead, 0, RowData.BlockHead.Length);
                    stream.Write(BitConverter.GetBytes(blockSize), 0, sizeof (Int32));
                    stream.Write(buffer.ToArray(), 0, blockSize);
                    stream.Write(RowData.StreamEnd, 0, RowData.StreamEnd.Length);
                }
            }
            else
            {
                Serializer.SerializeWithLengthPrefix(stream, dataResponse, PrefixStyle.Base128);
            }
        }
    }
}