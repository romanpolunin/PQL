using System;
using System.Data;
using System.IO;
using System.Threading;
using ProtoBuf;

namespace Pql.ClientDriver.Protocol
{
    internal sealed class PqlProtocolUtility : IDisposable
    {
        public PqlDataConnection Connection;
        public Stream InputStream;
        
        private IDisposable[] m_holders;

        /// <summary>
        /// Reads <see cref="DataResponse"/> from input stream referenced by <see cref="InputStream"/>.
        /// </summary>
        public DataResponse ReadResponse()
        {
            return ReadResponseHeaders(InputStream);
        }

        public DataResponse ReadResponseHeaders(Stream stream)
        {
            DataResponse response;
            try
            {
                response = Serializer.DeserializeWithLengthPrefix<DataResponse>(stream, PrefixStyle.Base128);
            }
            catch (Exception e)
            {
                throw new DataException("Could not read header from incoming stream", e);
            }

            if (response == null)
            {
                throw new DataException("Incoming response header is null");
            }

            if (response.ErrorCode != 0)
            {
                throw new DataException(string.Format(
                    "Server error code: {0}, message: {1}", response.ErrorCode, response.ServerMessage));
            }
            return response;
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="connection">Parent connection</param>
        /// <param name="inputStream">Incoming data stream from server</param>
        /// <param name="holders">Array of holder objects that must be disposed when reader is closed</param>
        public PqlProtocolUtility(PqlDataConnection connection, Stream inputStream, params IDisposable[] holders)
        {
            Connection = connection ?? throw new ArgumentNullException("connection");
            InputStream = inputStream ?? throw new ArgumentNullException("inputStream");
            m_holders = holders;
        }

        /// <summary>
        /// Releases the managed resources used by the <see cref="T:System.Data.Common.DbDataReader"/> and optionally releases the unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            var conn = Interlocked.CompareExchange(ref Connection, null, Connection);
            var holders = Interlocked.CompareExchange(ref m_holders, null, m_holders);

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