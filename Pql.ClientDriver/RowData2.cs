using System.Data;

using Pql.ClientDriver.Protocol;

namespace Pql.ClientDriver
{
    public class PqlMarshallerFactory: MarshallerFactory

    public struct Offsets
    {
        public short First16;
        public short First32;
        public short First64;
        public short First128;
        public short FirstStrings;
        public short CountBinaries;
    }

    public sealed class RowData2
    {
        public Protocol.Wire.PqlDataRow Raw;
        public readonly Offsets Structure;

        private static readonly Dictionary<DbType, Type> s_fieldTypeToNativeType;
        private static readonly Dictionary<DbType, ushort> s_dbTypeSizes;
        private readonly int[] _fieldCountsByStorageType;

        /// <summary>
        /// Offsets of each field stored in <see cref="Structured"/> buffer. 
        /// Indexes correspond to every field's ordinal.
        /// </summary>
        public readonly int[] FieldArrayIndexes;
        /// <summary>
        /// NotNulls bitvector, indexes correspond to every field's ordinal.
        /// </summary>
        public readonly int[] NotNulls;
        /// <summary>
        /// Data types, indexes correspond to every field's ordinal.
        /// </summary>
        public readonly DbType[] FieldTypes;
        /// <summary>
        /// Storage sizes, indexes correspond to every field's ordinal.
        /// </summary>
        public readonly ushort[] FieldSizes;
        /// <summary>
        /// Data for string types, indexes correspond to special mapping of field's ordinal to this array.
        /// </summary>
        public readonly SizableArray<char>[]? StringData;
        /// <summary>
        /// Data for binary types, indexes correspond to special mapping of field's ordinal to this array.
        /// </summary>
        public readonly SizableArray<byte>[]? BinaryData;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="fieldTypes">Field metadata</param>
        public RowData2(DbType[] fieldTypes)
        {
            if (fieldTypes == null)
            {
                throw new ArgumentNullException(nameof(fieldTypes));
            }

            NotNulls = new int[BitVector.GetArrayLength(fieldTypes.Length)];
            BitVector.Get(Raw.BytesData.);

            StreamObserver

            var offsets = new Offsets();

            FieldArrayIndexes = new int[fieldTypes.Length];
            FieldRepresentationTypes = new DataTypeRepresentation[fieldTypes.Length];

            _fieldCountsByStorageType = new int[1 + Enum.GetValues(typeof(DataTypeRepresentation)).Cast<byte>().Max()];

            int count;
            for (var ordinal = 0; ordinal < fieldTypes.Length; ordinal++)
            {
                var dbType = fieldTypes[ordinal];
                var storageSize = s_dbTypeSizes[dbType];
                count = _fieldCountsByStorageType[(byte)storageSize];

                FieldArrayIndexes[ordinal] = count;
                FieldRepresentationTypes[ordinal] = storageSize;
                _fieldCountsByStorageType[(byte)storageSize] = count + 1;
            }

            count = _fieldCountsByStorageType[(int)DataTypeRepresentation.Value8Bytes];
            ValueData8Bytes = count > 0 ? new ValueHolder8Bytes[count] : null;
            count = _fieldCountsByStorageType[(int)DataTypeRepresentation.Value16Bytes];
            ValueData16Bytes = count > 0 ? new ValueHolder16Bytes[count] : null;
            count = _fieldCountsByStorageType[(int)DataTypeRepresentation.CharArray];
            StringData = count > 0 ? new SizableArray<char>[count] : null;
            count = _fieldCountsByStorageType[(int)DataTypeRepresentation.ByteArray];
            BinaryData = count > 0 ? new SizableArray<byte>[count] : null;

            if (StringData != null)
            {
                for (var i = 0; i < StringData.Length; i++)
                {
                    StringData[i] = new SizableArray<char>();
                }
            }

            if (BinaryData != null)
            {
                for (var i = 0; i < BinaryData.Length; i++)
                {
                    BinaryData[i] = new SizableArray<byte>();
                }
            }

            FieldTypes = fieldTypes;
        }

        /// <summary>
        /// Returns index of the field in a typed array.
        /// </summary>
        /// <param name="indexInResponse">Index of the field in the response, irrespective of its data type. Also known as Ordinal: <see cref="DataResponseField.Ordinal"/></param>
        public int GetIndexInArray(int indexInResponse) => FieldArrayIndexes[indexInResponse];

        /// <summary>
        /// Returns integral value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public long GetInt64(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsInt64 : 0;

        /// <summary>
        /// Returns integral value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public int GetInt32(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsInt32 : 0;

        /// <summary>
        /// Returns integral value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public short GetInt16(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsInt16 : (short)0;

        /// <summary>
        /// Returns integral value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public byte GetByte(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsByte : (byte)0;

        /// <summary>
        /// Returns integral value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public bool GetBoolean(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) && ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsBoolean;

        /// <summary>
        /// Returns datetime value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public DateTime GetDateTime(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsDateTime : new DateTime();

        /// <summary>
        /// Returns datetime value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public DateTimeOffset GetDateTimeOffset(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData16Bytes[FieldArrayIndexes[indexInResponse]].AsDateTimeOffset : new DateTimeOffset();

        /// <summary>
        /// Returns string value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public string? GetString(int indexInResponse)
        {
            if (BitVector.Get(NotNulls, indexInResponse))
            {
                var data = StringData[FieldArrayIndexes[indexInResponse]];
                return data.Data == null
                    ? null
                    : data.Data.Length == 0
                    ? string.Empty
                    : new string(data.Data, 0, data.Length);
            }

            return null;
        }

        /// <summary>
        /// Returns single-character value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        /// <exception cref="DataException">String data must have exactly one character</exception>
        public char GetChar(int indexInResponse)
        {
            if (BitVector.Get(NotNulls, indexInResponse))
            {
                var data = StringData[FieldArrayIndexes[indexInResponse]];
                if (data.Length != 1)
                {
                    throw new DataException("Character array length must be equal to 1. Actual length: " + data.Length);
                }

                return data.Data[0];
            }

            return '\0';
        }

        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <returns>
        /// The actual number of characters read.
        /// </returns>
        /// <param name="i">The zero-based column ordinal. </param><param name="fieldoffset">The index within the row from which to start the read operation. </param>
        /// <param name="buffer">The buffer into which to read the stream of bytes. </param>
        /// <param name="bufferoffset">The index for <paramref name="buffer"/> to start the read operation. </param>
        /// <param name="length">The number of bytes to read. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
        {
            if (buffer == null)
            {
                return 0;
            }

            if (fieldoffset is > int.MaxValue or < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(fieldoffset));
            }

            if (bufferoffset is > int.MaxValue or < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferoffset));
            }

            if (length <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            if (BitVector.Get(NotNulls, i))
            {
                var data = StringData[FieldArrayIndexes[i]];
                int toCopy = (int)Math.Min(length, Math.Min(buffer.Length - bufferoffset, data.Length - fieldoffset));
                if (toCopy > 0)
                {
                    Buffer.BlockCopy(data.Data, sizeof(char) * (int)fieldoffset, buffer, sizeof(char) * bufferoffset, sizeof(char) * toCopy);
                    return toCopy;
                }
            }

            return 0;
        }

        /// <summary>
        /// Returns floating point value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public double GetDouble(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsDouble : 0;

        /// <summary>
        /// Returns floating point value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public float GetSingle(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsSingle : 0;

        /// <summary>
        /// Returns floating point value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public float GetFloat(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData8Bytes[FieldArrayIndexes[indexInResponse]].AsSingle : 0;

        /// <summary>
        /// Returns guid value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public Guid GetGuid(int indexInResponse) => BitVector.Get(NotNulls, indexInResponse) ? ValueData16Bytes[FieldArrayIndexes[indexInResponse]].AsGuid : Guid.Empty;

        /// <summary>
        /// Returns binary value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public byte[]? GetBinary(int indexInResponse)
        {
            if (BitVector.Get(NotNulls, indexInResponse))
            {
                var data = BinaryData[FieldArrayIndexes[indexInResponse]];
                var len = data.Length;
                if (len > 0)
                {
                    var result = new byte[len];
                    Buffer.BlockCopy(data.Data, 0, result, 0, len);
                    return result;
                }

                return null;
            }

            return null;
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <returns>
        /// The actual number of bytes read.
        /// </returns>
        /// <param name="i">The zero-based column ordinal. </param>
        /// <param name="fieldoffset">The index within the field from which to start the read operation. </param>
        /// <param name="buffer">The buffer into which to read the stream of bytes. </param>
        /// <param name="bufferoffset">The index for <paramref name="buffer"/> to start the read operation. </param>
        /// <param name="length">The number of bytes to read. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>
        public int GetBinary(int i, long fieldoffset, byte[]? buffer, int bufferoffset, int length)
        {
            if (buffer == null)
            {
                return 0;
            }

            if (fieldoffset is > int.MaxValue or < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(fieldoffset), fieldoffset, "Invalid offset");
            }

            if (bufferoffset is > int.MaxValue or < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bufferoffset), bufferoffset, "Invalid offset");
            }

            if (length <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(length));
            }

            if (BitVector.Get(NotNulls, i))
            {
                var data = BinaryData[FieldArrayIndexes[i]];
                int toCopy = (int)Math.Min(length, Math.Min(buffer.Length - bufferoffset, data.Length - fieldoffset));
                if (toCopy > 0)
                {
                    Buffer.BlockCopy(data.Data, (int)fieldoffset, buffer, bufferoffset, toCopy);
                    return toCopy;
                }
            }

            return 0;
        }

        /// <summary>
        /// Returns decimal value for the give field's ordinal, or default/null value if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public decimal GetCurrency(int indexInResponse) =>
            BitVector.Get(NotNulls, indexInResponse) ? ValueData16Bytes[FieldArrayIndexes[indexInResponse]].AsDecimal : 0;

        /// <summary>
        /// Returns object-boxed value for the give field's ordinal, or null object if it is marked as null in response.
        /// </summary>
        /// <param name="indexInResponse">Field's ordinal</param>
        public object GetValue(int indexInResponse)
        {
            if (!BitVector.Get(NotNulls, indexInResponse))
            {
                return DBNull.Value;
            }

            var indexInArray = FieldArrayIndexes[indexInResponse];
            object? result = FieldTypes[indexInResponse] switch
            {
                DbType.Object or DbType.Binary => GetBinary(indexInResponse),
                DbType.SByte => ValueData8Bytes[indexInArray].AsSByte,
                DbType.Byte => ValueData8Bytes[indexInArray].AsByte,
                DbType.Boolean => ValueData8Bytes[indexInArray].AsBoolean,
                DbType.Decimal or DbType.Currency => ValueData16Bytes[indexInArray].AsDecimal,
                DbType.Double => ValueData8Bytes[indexInArray].AsDouble,
                DbType.Guid => GetGuid(indexInResponse),
                DbType.Int16 => ValueData8Bytes[indexInArray].AsInt16,
                DbType.UInt16 => ValueData8Bytes[indexInArray].AsUInt16,
                DbType.Int32 => ValueData8Bytes[indexInArray].AsInt32,
                DbType.UInt32 => ValueData8Bytes[indexInArray].AsUInt32,
                DbType.Date or DbType.DateTime or DbType.DateTime2 => ValueData8Bytes[indexInArray].AsDateTime,
                DbType.Time => ValueData8Bytes[indexInArray].AsTimeSpan,
                DbType.DateTimeOffset => ValueData16Bytes[indexInArray].AsDateTimeOffset,
                DbType.Int64 => ValueData8Bytes[indexInArray].AsInt64,
                DbType.UInt64 => ValueData8Bytes[indexInArray].AsUInt64,
                DbType.Single => ValueData8Bytes[indexInArray].AsSingle,
                DbType.String or DbType.AnsiString or DbType.AnsiStringFixedLength or DbType.StringFixedLength or DbType.Xml => GetString(indexInResponse),
                _ => throw new DataException("Invalid DbType: " + FieldTypes[indexInResponse]),
            };

            return result ?? DBNull.Value;
        }

        /// <summary>
        /// Returns array of object values, as many as can fit in <paramref name="values"/>.
        /// </summary>
        /// <param name="values">Destination array</param>
        /// <returns>Number of values written to array</returns>
        public int GetValues(object[] values)
        {
            if (values == null || values.Length == 0)
            {
                return 0;
            }

            var toCopy = Math.Min(values.Length, FieldArrayIndexes.Length);
            for (var i = 0; i < toCopy; i++)
            {
                values[i] = GetValue(i) ?? DBNull.Value;
            }

            return toCopy;
        }

        /// <summary>
        /// Attempts to read next row from the data stream.  
        /// </summary>
        /// <returns>True is successful and row data is available for retrieval with Getxxx methods. False otherwise.</returns> 
        public void Read(BinaryReader reader)
        {
            BitVector.Read(NotNulls, FieldTypes.Length, reader);

            for (var indexInResponse = 0; indexInResponse < FieldArrayIndexes.Length; indexInResponse++)
            {
                if (!BitVector.Get(NotNulls, indexInResponse))
                {
                    continue;
                }

                var indexInArray = FieldArrayIndexes[indexInResponse];
                switch (FieldTypes[indexInResponse])
                {
                    case DbType.Object:
                    case DbType.Binary:
                        {
                            var data = BinaryData[indexInArray];
                            data.SetLength(Read7BitEncodedInt(reader));
                            var bytesRead = 0;
                            while (bytesRead < data.Length)
                            {
                                var count = reader.Read(data.Data, bytesRead, data.Length - bytesRead);
                                if (count == 0)
                                {
                                    throw new DataException("Unexpected end of stream");
                                }

                                bytesRead += count;
                            }
                        }

                        break;
                    case DbType.SByte:
                    case DbType.Byte:
                        ValueData8Bytes[indexInArray].AsByte = reader.ReadByte();
                        break;
                    case DbType.Boolean:
                        ValueData8Bytes[indexInArray].AsBoolean = reader.ReadBoolean();
                        break;
                    case DbType.Decimal:
                    case DbType.Currency:
                    case DbType.Guid:
                    case DbType.DateTimeOffset:
                        ValueData16Bytes[indexInArray].Lo = reader.ReadInt64();
                        ValueData16Bytes[indexInArray].Hi = reader.ReadInt64();
                        break;
                    case DbType.Int16:
                    case DbType.UInt16:
                        ValueData8Bytes[indexInArray].AsInt16 = reader.ReadInt16();
                        break;
                    case DbType.Int32:
                    case DbType.UInt32:
                    case DbType.Single:
                        ValueData8Bytes[indexInArray].AsInt32 = reader.ReadInt32();
                        break;
                    case DbType.Date:
                    case DbType.DateTime:
                    case DbType.DateTime2:
                    case DbType.Time:
                    case DbType.Int64:
                    case DbType.UInt64:
                    case DbType.Double:
                        ValueData8Bytes[indexInArray].AsInt64 = reader.ReadInt64();
                        break;
                    case DbType.String:
                    case DbType.AnsiString:
                    case DbType.AnsiStringFixedLength:
                    case DbType.StringFixedLength:
                    case DbType.Xml:
                        {
                            var data = StringData[indexInArray];
                            var len = Read7BitEncodedInt(reader);
                            data.SetLength(len);
                            for (var i = 0; i < len; i++)
                            {
                                data.Data[i] = (char)Read7BitEncodedInt(reader);
                            }
                        }

                        break;

                    case DbType.VarNumeric:
                    default:
                        throw new DataException("Invalid DbType: " + FieldTypes[indexInResponse]);
                }
            }
        }

        /// <summary>
        /// Writes row data into output stream.
        /// </summary>
        public void Write(BinaryWriter writer)
        {
            BitVector.Write(NotNulls, FieldTypes.Length, writer);

            for (var indexInResponse = 0; indexInResponse < FieldArrayIndexes.Length; indexInResponse++)
            {
                if (!BitVector.Get(NotNulls, indexInResponse))
                {
                    continue;
                }

                var indexInArray = FieldArrayIndexes[indexInResponse];

                switch (FieldTypes[indexInResponse])
                {
                    case DbType.Object:
                    case DbType.Binary:
                        {
                            var data = BinaryData[indexInArray];
                            var len = data.Length;
                            Write7BitEncodedInt(writer, len);
                            if (len > 0)
                            {
                                writer.Write(data.Data, 0, data.Length);
                            }
                        }

                        break;
                    case DbType.SByte:
                    case DbType.Byte:
                        writer.Write(ValueData8Bytes[indexInArray].AsByte);
                        break;
                    case DbType.Boolean:
                        writer.Write(ValueData8Bytes[indexInArray].AsBoolean);
                        break;
                    case DbType.Decimal:
                    case DbType.Currency:
                    case DbType.Guid:
                    case DbType.DateTimeOffset:
                        writer.Write(ValueData16Bytes[indexInArray].Lo);
                        writer.Write(ValueData16Bytes[indexInArray].Hi);
                        break;
                    case DbType.Int16:
                    case DbType.UInt16:
                        writer.Write(ValueData8Bytes[indexInArray].AsInt16);
                        break;
                    case DbType.Int32:
                    case DbType.UInt32:
                    case DbType.Single:
                        writer.Write(ValueData8Bytes[indexInArray].AsInt32);
                        break;
                    case DbType.Date:
                    case DbType.DateTime:
                    case DbType.DateTime2:
                    case DbType.Time:
                    case DbType.Int64:
                    case DbType.UInt64:
                    case DbType.Double:
                        writer.Write(ValueData8Bytes[indexInArray].AsInt64);
                        break;
                    case DbType.String:
                    case DbType.AnsiString:
                    case DbType.AnsiStringFixedLength:
                    case DbType.StringFixedLength:
                    case DbType.Xml:
                        {
                            var data = StringData[indexInArray];
                            Write7BitEncodedInt(writer, data.Length);
                            for (var i = 0; i < data.Length; i++)
                            {
                                Write7BitEncodedInt(writer, data.Data[i]);
                            }
                        }

                        break;

                    case DbType.VarNumeric:
                    default:
                        throw new DataException("Invalid DbType: " + FieldTypes[indexInResponse]);
                }
            }
        }

        private void Clear()
        {
            if (ValueData8Bytes != null)
            {
                Array.Clear(ValueData8Bytes, 0, ValueData8Bytes.Length);
            }

            if (ValueData16Bytes != null)
            {
                Array.Clear(ValueData16Bytes, 0, ValueData16Bytes.Length);
            }

            if (StringData != null)
            {
                foreach (var item in StringData)
                {
                    item.SetLength(0);
                }
            }

            if (BinaryData != null)
            {
                foreach (var item in BinaryData)
                {
                    item.SetLength(0);
                }
            }
        }

        static RowData()
        {
            s_dbTypeSizes = new Dictionary<DbType, ushort>
                {
                    {DbType.AnsiString, 0},
                    {DbType.AnsiStringFixedLength, 0},
                    {DbType.Binary, 0},
                    {DbType.Object, 0},
                    {DbType.Boolean, 1},
                    {DbType.Byte, 1},
                    {DbType.Currency, 16},
                    {DbType.Date, 8},
                    {DbType.DateTime, 8},
                    {DbType.DateTime2, 8},
                    {DbType.DateTimeOffset, 16},
                    {DbType.Decimal, 16},
                    {DbType.Double, 8},
                    {DbType.Guid, 16},
                    {DbType.Int16, 2},
                    {DbType.Int32, 4},
                    {DbType.Int64, 8},
                    {DbType.SByte, 1},
                    {DbType.Single, 4},
                    {DbType.String, 0},
                    {DbType.StringFixedLength, 0},
                    {DbType.Time, 8},
                    {DbType.UInt16, 2},
                    {DbType.UInt32, 4},
                    {DbType.UInt64, 8},
                    {DbType.Xml, 0}
                };

            //FieldTypes.Add(DbType.VarNumeric, null);
            s_fieldTypeToNativeType = new Dictionary<DbType, Type>
                {
                    {DbType.AnsiString, typeof (string)},
                    {DbType.AnsiStringFixedLength, typeof (string)},
                    {DbType.Binary, typeof (byte[])},
                    {DbType.Object, typeof (byte[])},
                    {DbType.Boolean, typeof (bool)},
                    {DbType.Byte, typeof (byte)},
                    {DbType.Currency, typeof (decimal)},
                    {DbType.Date, typeof (DateTime)},
                    {DbType.DateTime, typeof (DateTime)},
                    {DbType.DateTime2, typeof (DateTime)},
                    {DbType.DateTimeOffset, typeof (DateTimeOffset)},
                    {DbType.Decimal, typeof (decimal)},
                    {DbType.Double, typeof (double)},
                    {DbType.Guid, typeof (Guid)},
                    {DbType.Int16, typeof (short)},
                    {DbType.Int32, typeof (int)},
                    {DbType.Int64, typeof (long)},
                    {DbType.SByte, typeof (sbyte)},
                    {DbType.Single, typeof (float)},
                    {DbType.String, typeof (string)},
                    {DbType.StringFixedLength, typeof (string)},
                    {DbType.Time, typeof (TimeSpan)},
                    {DbType.UInt16, typeof (ushort)},
                    {DbType.UInt32, typeof (uint)},
                    {DbType.UInt64, typeof (ulong)},
                    {DbType.Xml, typeof (string)}
                };
        }

        /// <summary>
        /// Disassembled method from BinaryReader.
        /// </summary>
        public static int Read7BitEncodedInt(BinaryReader reader)
        {
            int num1 = 0;
            int num2 = 0;
            while (num2 != 35)
            {
                byte num3 = reader.ReadByte();
                num1 |= (num3 & sbyte.MaxValue) << num2;
                num2 += 7;
                if ((num3 & 128) == 0)
                    return num1;
            }

            throw new FormatException("Bad 7Bit-encoded Int32");
        }

        /// <summary>
        /// Disassembled method from BinaryWriter.
        /// </summary>
        public static void Write7BitEncodedInt(BinaryWriter writer, int value)
        {
            uint num = (uint)value;
            while (num >= 128U)
            {
                writer.Write((byte)(num | 128U));
                num >>= 7;
            }

            writer.Write((byte)num);
        }

        /// <summary>
        /// Attempts to find a system type based on provided logical data type.
        /// </summary>
        /// <param name="dbType">Logical data type</param>
        /// <returns>System type</returns>
        /// <exception cref="ArgumentOutOfRangeException">Unknown data type</exception>
        public static Type DeriveSystemType(DbType dbType)
        {
            if (!s_fieldTypeToNativeType.TryGetValue(dbType, out var result))
            {
                throw new ArgumentOutOfRangeException(nameof(dbType), dbType, "Unknown data type");
            }

            return result;
        }
    }
}