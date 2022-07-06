using System.Buffers.Binary;

namespace Pql.ClientDriver
{
    public ref struct SpanWriter
    {
        private Span<byte> _buffer;

        public SpanWriter(Span<byte> buffer)
        {
            _buffer = buffer;
        }

        public void Write(bool b)
        {
            _buffer[0] = b ? (byte)1 : (byte)0;
            _buffer = _buffer[sizeof(byte)..];
        }

        public void Write(byte b)
        {
            _buffer[0] = b;
            _buffer = _buffer[sizeof(byte)..];
        }

        public void Write(sbyte b)
        {
            _buffer[0] = unchecked((byte)b);
            _buffer = _buffer[sizeof(byte)..];
        }

        public void Write(Int16 v)
        {
            BinaryPrimitives.WriteInt16BigEndian(_buffer, v);
            _buffer = _buffer[sizeof(Int16)..];
        }

        public void Write(UInt16 v)
        {
            BinaryPrimitives.WriteUInt16BigEndian(_buffer, v);
            _buffer = _buffer[sizeof(UInt16)..];
        }

        public void Write(Int32 v)
        {
            BinaryPrimitives.WriteInt32BigEndian(_buffer, v);
            _buffer = _buffer[sizeof(Int32)..];
        }

        public void Write(UInt32 v)
        {
            BinaryPrimitives.WriteUInt32BigEndian(_buffer, v);
            _buffer = _buffer[sizeof(UInt32)..];
        }

        public void Write(Int64 v)
        {
            BinaryPrimitives.WriteInt64BigEndian(_buffer, v);
            _buffer = _buffer[sizeof(Int64)..];
        }

        public void Write(UInt64 v)
        {
            BinaryPrimitives.WriteUInt64BigEndian(_buffer, v);
            _buffer = _buffer[sizeof(UInt64)..];
        }
    }
}
