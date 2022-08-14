using System.Data;
using System.Text;

using Pql.ExpressionEngine.Interfaces;
using Pql.SqlEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;
using Pql.SqlEngine.Interfaces.Services;

namespace Pql.SqlEngine.DataContainer.Engine
{
    /// <summary>
    /// Utility class that reads incoming stream of <see cref="RowData"/> items one item at a time 
    /// and puts field values into supplied <see cref="DriverRowData"/> buffer.
    /// </summary>
    internal class InputDataStreamEnumerator : IDriverDataEnumerator
    {
        private readonly int _countToRead;
        private readonly DriverRowData _driverRowData;
        private readonly BinaryReader _reader;
        private readonly RowData _readerBuffer;
        private int _readSoFar;
        private readonly RowData.DataTypeRepresentation _pkFieldType;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="countToRead">Number of <see cref="RowData"/> items to read, can be zero</param>
        /// <param name="fieldTypes">Expected field types</param>
        /// <param name="stream">Incoming data stream</param>
        /// <param name="driverRowData">Buffer to put field values into, after each call to <see cref="MoveNext"/></param>
        public InputDataStreamEnumerator(int countToRead, DbType[] fieldTypes, Stream stream, DriverRowData driverRowData)
        {
            if (countToRead < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(countToRead), countToRead, "Count may not be negative");
            }

            if (fieldTypes == null)
            {
                throw new ArgumentNullException(nameof(fieldTypes));
            }

            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            if (driverRowData == null)
            {
                throw new ArgumentNullException(nameof(driverRowData));
            }
            
            if (fieldTypes.Length == 0)
            {
                throw new ArgumentException("Count of fields in client row buffer must be greater than zero, first column must contain primary key value");
            }

            if (driverRowData.FieldTypes.Length != fieldTypes.Length)
            {
                throw new ArgumentException(string.Format("Count of fields in client row buffer ({0}) must be equal to count of fields in driver buffer ({1})"
                    , fieldTypes.Length, driverRowData.FieldTypes.Length));
            }

            for (var ordinal = 0; ordinal < driverRowData.FieldTypes.Length; ordinal++)
            {
                if (driverRowData.FieldTypes[ordinal] != fieldTypes[ordinal])
                {
                    throw new ArgumentException(string.Format("Field type mismatch at ordinal {0}. Client type: {1}, driver type: {2}",
                        ordinal, fieldTypes[ordinal], _driverRowData.FieldTypes[ordinal]));
                }
            }

            _countToRead = countToRead;
            _driverRowData = driverRowData;
            _reader = new BinaryReader(stream, Encoding.UTF8, true);
            _readerBuffer = new RowData(fieldTypes);
            _pkFieldType = _readerBuffer.FieldRepresentationTypes[0];
        }

        public bool MoveNext()
        {
            if (_readSoFar >= _countToRead)
            {
                return false;
            }

            if (!_readerBuffer.Read(_reader))
            {
                throw new Exception(string.Format(
                    "Failed to advance. Current count: {0}, expected count: {1}", _readSoFar, _countToRead));
            }

            ReadFromClientRowData();

            ReadPrimaryKey();

            _readSoFar++;
            return _readSoFar <= _countToRead;
        }

        private void ReadPrimaryKey()
        {
            if (!BitVector.Get(_readerBuffer.NotNulls, 0))
            {
                throw new Exception("Primary key value may not be null");
            }

            var indexInArray = _readerBuffer.FieldArrayIndexes[0];
            var internalEntityId = _driverRowData.InternalEntityId;
            
            switch (_pkFieldType)
            {
                case RowData.DataTypeRepresentation.ByteArray:
                    {
                        var value = _readerBuffer.BinaryData[indexInArray];
                        if (value.Length is 0 or > byte.MaxValue)
                        {
                            throw new Exception("Primary key length must be within 1 to 255 bytes");
                        }

                        Buffer.BlockCopy(value.Data, 0, internalEntityId, 1, value.Length-1);
                        internalEntityId[0] = (byte)(value.Length-1);
                    }
                    break;
                case RowData.DataTypeRepresentation.CharArray:
                    {
                        var value = _readerBuffer.StringData[indexInArray];
                        if (value.Length is 0 or > byte.MaxValue)
                        {
                            throw new Exception("Primary key length must be within 1 to 255 characters");
                        }

                        var bytelen = Encoding.UTF8.GetByteCount(value.Data, 0, value.Length);
                        if (bytelen is 0 or > byte.MaxValue)
                        {
                            throw new Exception("UTF conversion must produce from 1 to 255 bytes");
                        }
                        internalEntityId[0] = (byte)Encoding.UTF8.GetBytes(value.Data, 0, value.Length, internalEntityId, 1);
                    }
                    break;
                case RowData.DataTypeRepresentation.Value8Bytes:
                    {
                        var value = _readerBuffer.ValueData8Bytes[indexInArray].AsUInt64;
                        var pos = 1;
                        while (value > 0)
                        {
                            internalEntityId[pos] = (byte) value;
                            value >>= 8;
                            pos++;
                        }
                        internalEntityId[0] = (byte)(pos-1);
                    }
                    break;
                case RowData.DataTypeRepresentation.Value16Bytes:
                    {
                        var value = (ulong)_readerBuffer.ValueData16Bytes[indexInArray].Lo;
                        var pos = 1;
                        while (value > 0)
                        {
                            internalEntityId[pos] = (byte)value;
                            value >>= 8;
                            pos++;
                        }
                        value = (ulong)_readerBuffer.ValueData16Bytes[indexInArray].Hi;
                        while (value > 0)
                        {
                            internalEntityId[pos] = (byte)value;
                            value >>= 8;
                            pos++;
                        }
                        internalEntityId[0] = (byte)(pos-1);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        
        private void Write8Bytes(long value, byte[] buffer, int offset)
        {
            for (var i = 0; i < 8; i++)
            {
                buffer[i + offset] = (byte)value;
                value >>= 8;
            }
        }

        public void FetchAdditionalFields()
        {
            // we fetch all fields in MoveNext, so do nothing
        }

        public DriverRowData Current => _driverRowData;


        public void FetchInternalEntityIdIntoChangeBuffer(DriverChangeBuffer changeBuffer, RequestExecutionContext context)
        {
            // reference copy is safe because storage driver is responsible for copying this value 
            // when it reads the change buffer
            changeBuffer.InternalEntityId = context.DriverOutputBuffer.InternalEntityId;
        }

        public void Dispose()
        {
            
        }

        
        private void ReadFromClientRowData()
        {
            for (var i = 0; i < _driverRowData.NotNulls.Length; i++)
            {
                _driverRowData.NotNulls[i] = _readerBuffer.NotNulls[i];
            }

            for (var ordinal = 0; ordinal < _readerBuffer.FieldTypes.Length; ordinal++)
            {
                var indexInArray = _readerBuffer.GetIndexInArray(ordinal);

                if (!BitVector.Get(_driverRowData.NotNulls, ordinal))
                {
                    continue;
                }

                switch (_readerBuffer.FieldRepresentationTypes[ordinal])
                {
                    case RowData.DataTypeRepresentation.ByteArray:
                        {
                            var dest = _driverRowData.BinaryData[indexInArray];
                            if (dest == null)
                            {
                                dest = new SizableArrayOfByte();
                                _driverRowData.BinaryData[indexInArray] = dest;
                            }

                            var src = _readerBuffer.BinaryData[indexInArray];
                            dest.SetLength(src.Length);
                            if (src.Length > 0)
                            {
                                Buffer.BlockCopy(src.Data, 0, dest.Data, 0, src.Length);
                            }
                        }
                        break;
                    case RowData.DataTypeRepresentation.CharArray:
                        {
                            var src = _readerBuffer.StringData[indexInArray];
                            _driverRowData.StringData[indexInArray] = src == null || src.Length == 0 ? string.Empty : new string(src.Data, 0, src.Length);
                        }
                        break;
                    case RowData.DataTypeRepresentation.Value8Bytes:
                        _driverRowData.ValueData8Bytes[indexInArray].AsInt64 = _readerBuffer.ValueData8Bytes[indexInArray].AsInt64;
                        break;
                    case RowData.DataTypeRepresentation.Value16Bytes:
                        _driverRowData.ValueData16Bytes[indexInArray].Lo = _readerBuffer.ValueData16Bytes[indexInArray].Lo;
                        _driverRowData.ValueData16Bytes[indexInArray].Hi = _readerBuffer.ValueData16Bytes[indexInArray].Hi;
                        break;
                    default:
                        throw new InvalidOperationException("Invalid representation type: " + _readerBuffer.FieldRepresentationTypes[ordinal]);
                }
            }
        }
    }
}