using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Pql.ClientDriver
{
    public ref struct SpanReader
    {
        private ReadOnlySpan<byte> _data;

        public SpanReader(ReadOnlySpan<byte> data)
        {
            _data = data;
        }

        public bool ReadBool()
        {
            var result = _data[0] != 0;
            _data = _data[sizeof(byte)..];
            return result;
        }

        public byte ReadByte()
        {
            var result = _data[0];
            _data = _data[sizeof(byte)..];
            return result;
        }

        public sbyte ReadSByte()
        {
            var result = unchecked((sbyte)_data[0]);
            _data = _data[sizeof(byte)..];
            return result;
        }

        public Int16 ReadInt16()
        {
            var result = BinaryPrimitives.ReadInt16BigEndian(_data);
            _data = _data[sizeof(Int16)..];
            return result;
        }

        public UInt16 ReadUInt16()
        {
            var result = BinaryPrimitives.ReadUInt16BigEndian(_data);
            _data = _data[sizeof(UInt16)..];
            return result;
        }

        public Int32 ReadInt32()
        {
            var result = BinaryPrimitives.ReadInt32BigEndian(_data);
            _data = _data[sizeof(Int32)..];
            return result;
        }

        public UInt32 ReadUInt32()
        {
            var result = BinaryPrimitives.ReadUInt32BigEndian(_data);
            _data = _data[sizeof(UInt32)..];
            return result;
        }

        public Int64 ReadInt64()
        {
            var result = BinaryPrimitives.ReadInt64BigEndian(_data);
            _data = _data[sizeof(Int64)..];
            return result;
        }

        public UInt64 ReadUInt64()
        {
            var result = BinaryPrimitives.ReadUInt64BigEndian(_data);
            _data = _data[sizeof(UInt64)..];
            return result;
        }
    }
}
