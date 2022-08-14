namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Utility class, used to write error information into output stream
    /// </summary>
    public sealed class PqlErrorDataWriter 
    {
        private readonly Exception _exception;
        private readonly int _errorCode;
        private readonly bool _writeBlockHeaderAndStreamEnd;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="errorCode">Optional error code</param>
        /// <param name="exception">Optional exception information</param>
        /// <param name="writeBlockHeaderAndStreamEnd">True to write a block header</param>
        public PqlErrorDataWriter(int errorCode, Exception exception, bool writeBlockHeaderAndStreamEnd)
        {
            _errorCode = errorCode;
            _exception = exception;
            _writeBlockHeaderAndStreamEnd = writeBlockHeaderAndStreamEnd;
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
        public void WriteTo(Stream stream) => throw new NotImplementedException();/*
            var dataResponse = new DataResponse(_errorCode, _exception == null ? "Unknown error" : _exception.Message);

            if (_writeBlockHeaderAndStreamEnd)
            {
                using var buffer = new MemoryStream();
                Serializer.SerializeWithLengthPrefix(buffer, dataResponse, PrefixStyle.Base128);

                var blockSize = checked((Int32)buffer.Length);
                stream.Write(RowData.BlockHead, 0, RowData.BlockHead.Length);
                stream.Write(BitConverter.GetBytes(blockSize), 0, sizeof(Int32));
                stream.Write(buffer.ToArray(), 0, blockSize);
                stream.Write(RowData.StreamEnd, 0, RowData.StreamEnd.Length);
            }
            else
            {
                Serializer.SerializeWithLengthPrefix(stream, dataResponse, PrefixStyle.Base128);
            }*/
    }
}