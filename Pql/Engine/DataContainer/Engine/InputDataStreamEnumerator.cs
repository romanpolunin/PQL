using System;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Pql.ClientDriver;
using Pql.ClientDriver.Protocol;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;
using Pql.ExpressionEngine.Interfaces;

namespace Pql.Engine.DataContainer.Engine
{
    /// <summary>
    /// Utility class that reads incoming stream of <see cref="RowData"/> items one item at a time 
    /// and puts field values into supplied <see cref="DriverRowData"/> buffer.
    /// </summary>
    internal class InputDataStreamEnumerator : IDriverDataEnumerator
    {
        private readonly int m_countToRead;
        private readonly DriverRowData m_driverRowData;
        private readonly BinaryReader m_reader;
        private readonly RowData m_readerBuffer;
        private int m_readSoFar;
        private readonly RowData.DataTypeRepresentation m_pkFieldType;

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
                throw new ArgumentOutOfRangeException("countToRead", countToRead, "Count may not be negative");
            }

            if (fieldTypes == null)
            {
                throw new ArgumentNullException("fieldTypes");
            }

            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (driverRowData == null)
            {
                throw new ArgumentNullException("driverRowData");
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
                        ordinal, fieldTypes[ordinal], m_driverRowData.FieldTypes[ordinal]));
                }
            }

            m_countToRead = countToRead;
            m_driverRowData = driverRowData;
            m_reader = new BinaryReader(stream, Encoding.UTF8, true);
            m_readerBuffer = new RowData(fieldTypes);
            m_pkFieldType = m_readerBuffer.FieldRepresentationTypes[0];
        }

        public bool MoveNext()
        {
            if (m_readSoFar >= m_countToRead)
            {
                return false;
            }

            if (!m_readerBuffer.Read(m_reader))
            {
                throw new Exception(string.Format(
                    "Failed to advance. Current count: {0}, expected count: {1}", m_readSoFar, m_countToRead));
            }

            ReadFromClientRowData();

            ReadPrimaryKey();

            m_readSoFar++;
            return m_readSoFar <= m_countToRead;
        }

        private void ReadPrimaryKey()
        {
            if (!BitVector.Get(m_readerBuffer.NotNulls, 0))
            {
                throw new Exception("Primary key value may not be null");
            }

            var indexInArray = m_readerBuffer.FieldArrayIndexes[0];
            var internalEntityId = m_driverRowData.InternalEntityId;
            
            switch (m_pkFieldType)
            {
                case RowData.DataTypeRepresentation.ByteArray:
                    {
                        var value = m_readerBuffer.BinaryData[indexInArray];
                        if (value.Length == 0 || value.Length > byte.MaxValue)
                        {
                            throw new Exception("Primary key length must be within 1 to 255 bytes");
                        }

                        Buffer.BlockCopy(value.Data, 0, internalEntityId, 1, value.Length-1);
                        internalEntityId[0] = (byte)(value.Length-1);
                    }
                    break;
                case RowData.DataTypeRepresentation.CharArray:
                    {
                        var value = m_readerBuffer.StringData[indexInArray];
                        if (value.Length == 0 || value.Length > byte.MaxValue)
                        {
                            throw new Exception("Primary key length must be within 1 to 255 characters");
                        }

                        var bytelen = Encoding.UTF8.GetByteCount(value.Data, 0, value.Length);
                        if (bytelen == 0 || bytelen > byte.MaxValue)
                        {
                            throw new Exception("UTF conversion must produce from 1 to 255 bytes");
                        }
                        internalEntityId[0] = (byte)Encoding.UTF8.GetBytes(value.Data, 0, value.Length, internalEntityId, 1);
                    }
                    break;
                case RowData.DataTypeRepresentation.Value8Bytes:
                    {
                        var value = m_readerBuffer.ValueData8Bytes[indexInArray].AsUInt64;
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
                        var value = (UInt64)m_readerBuffer.ValueData16Bytes[indexInArray].Lo;
                        var pos = 1;
                        while (value > 0)
                        {
                            internalEntityId[pos] = (byte)value;
                            value >>= 8;
                            pos++;
                        }
                        value = (UInt64)m_readerBuffer.ValueData16Bytes[indexInArray].Hi;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        public DriverRowData Current { get { return m_driverRowData; } }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FetchInternalEntityIdIntoChangeBuffer(DriverChangeBuffer changeBuffer, RequestExecutionContext context)
        {
            // reference copy is safe because storage driver is responsible for copying this value 
            // when it reads the change buffer
            changeBuffer.InternalEntityId = context.DriverOutputBuffer.InternalEntityId;
        }

        public void Dispose()
        {
            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReadFromClientRowData()
        {
            for (var i = 0; i < m_driverRowData.NotNulls.Length; i++)
            {
                m_driverRowData.NotNulls[i] = m_readerBuffer.NotNulls[i];
            }

            for (var ordinal = 0; ordinal < m_readerBuffer.FieldTypes.Length; ordinal++)
            {
                var indexInArray = m_readerBuffer.GetIndexInArray(ordinal);

                if (!BitVector.Get(m_driverRowData.NotNulls, ordinal))
                {
                    continue;
                }

                switch (m_readerBuffer.FieldRepresentationTypes[ordinal])
                {
                    case RowData.DataTypeRepresentation.ByteArray:
                        {
                            var dest = m_driverRowData.BinaryData[indexInArray];
                            if (dest == null)
                            {
                                dest = new SizableArrayOfByte();
                                m_driverRowData.BinaryData[indexInArray] = dest;
                            }

                            var src = m_readerBuffer.BinaryData[indexInArray];
                            dest.SetLength(src.Length);
                            if (src.Length > 0)
                            {
                                Buffer.BlockCopy(src.Data, 0, dest.Data, 0, src.Length);
                            }
                        }
                        break;
                    case RowData.DataTypeRepresentation.CharArray:
                        {
                            var src = m_readerBuffer.StringData[indexInArray];
                            m_driverRowData.StringData[indexInArray] = src == null || src.Length == 0 ? string.Empty : new string(src.Data, 0, src.Length);
                        }
                        break;
                    case RowData.DataTypeRepresentation.Value8Bytes:
                        m_driverRowData.ValueData8Bytes[indexInArray].AsInt64 = m_readerBuffer.ValueData8Bytes[indexInArray].AsInt64;
                        break;
                    case RowData.DataTypeRepresentation.Value16Bytes:
                        m_driverRowData.ValueData16Bytes[indexInArray].Lo = m_readerBuffer.ValueData16Bytes[indexInArray].Lo;
                        m_driverRowData.ValueData16Bytes[indexInArray].Hi = m_readerBuffer.ValueData16Bytes[indexInArray].Hi;
                        break;
                    default:
                        throw new InvalidOperationException("Invalid representation type: " + m_readerBuffer.FieldRepresentationTypes[ordinal]);
                }
            }
        }
    }
}