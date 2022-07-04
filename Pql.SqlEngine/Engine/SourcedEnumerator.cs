using System;
using System.Runtime.CompilerServices;
using System.Text;
using Pql.ClientDriver.Protocol;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;

namespace Pql.Engine.DataContainer.Engine
{
    internal class SourcedEnumerator : IDriverDataEnumerator
    {
        private int m_position = -1;
        private readonly DriverRowData.DataTypeRepresentation m_pkFieldType;

        public SourcedEnumerator(DriverRowData.DataTypeRepresentation pkFieldType)
        {
            m_pkFieldType = pkFieldType;
        }

        public bool MoveNext()
        {
            if (m_position == -1)
            {
                m_position++;
                return true;
            }
            
            return false;
        }

        public void FetchAdditionalFields()
        {
            
        }

        public void Dispose()
        {
            
        }

        public DriverRowData Current { get { throw new NotSupportedException(); } }

        
        public void FetchInternalEntityIdIntoChangeBuffer(DriverChangeBuffer changeBuffer, RequestExecutionContext context)
        {
            if (changeBuffer.InternalEntityId == null)
            {
                changeBuffer.InternalEntityId = new byte[context.DriverOutputBuffer.InternalEntityId.Length];
            }
            
            ReadPrimaryKey(changeBuffer);
        }

        private void ReadPrimaryKey(DriverChangeBuffer changeBuffer)
        {
            if (!BitVector.Get(changeBuffer.Data.NotNulls, 0))
            {
                throw new Exception("Primary key value may not be null");
            }

            var indexInArray = changeBuffer.Data.FieldArrayIndexes[0];
            var internalEntityId = changeBuffer.InternalEntityId;

            switch (m_pkFieldType)
            {
                case DriverRowData.DataTypeRepresentation.ByteArray:
                    {
                        var value = changeBuffer.Data.BinaryData[indexInArray];
                        if (value.Length == 0 || value.Length > byte.MaxValue)
                        {
                            throw new Exception("Primary key length must be within 1 to 255 bytes");
                        }

                        Buffer.BlockCopy(value.Data, 0, internalEntityId, 1, value.Length - 1);
                        internalEntityId[0] = (byte)(value.Length - 1);
                    }
                    break;
                case DriverRowData.DataTypeRepresentation.String:
                    {
                        var value = changeBuffer.Data.StringData[indexInArray];
                        if (value.Length == 0 || value.Length > byte.MaxValue)
                        {
                            throw new Exception("Primary key length must be within 1 to 255 characters");
                        }

                        var bytelen = Encoding.UTF8.GetByteCount(value);
                        if (bytelen == 0 || bytelen > byte.MaxValue)
                        {
                            throw new Exception("UTF conversion must produce from 1 to 255 bytes");
                        }
                        internalEntityId[0] = (byte)Encoding.UTF8.GetBytes(value, 0, value.Length, internalEntityId, 1);
                    }
                    break;
                case DriverRowData.DataTypeRepresentation.Value8Bytes:
                    {
                        var value = changeBuffer.Data.ValueData8Bytes[indexInArray].AsUInt64;
                        var pos = 1;
                        while (value > 0)
                        {
                            internalEntityId[pos] = (byte)value;
                            value >>= 8;
                            pos++;
                        }
                        internalEntityId[0] = (byte)(pos - 1);
                    }
                    break;
                case DriverRowData.DataTypeRepresentation.Value16Bytes:
                    {
                        var value = (UInt64)changeBuffer.Data.ValueData16Bytes[indexInArray].Lo;
                        var pos = 1;
                        while (value > 0)
                        {
                            internalEntityId[pos] = (byte)value;
                            value >>= 8;
                            pos++;
                        }
                        value = (UInt64)changeBuffer.Data.ValueData16Bytes[indexInArray].Hi;
                        while (value > 0)
                        {
                            internalEntityId[pos] = (byte)value;
                            value >>= 8;
                            pos++;
                        }
                        internalEntityId[0] = (byte)(pos - 1);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

    }
}
