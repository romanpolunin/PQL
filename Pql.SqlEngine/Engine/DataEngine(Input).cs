using System.Data;
using System.Text;

using Pql.SqlEngine.DataContainer.Parser;
using Pql.ExpressionEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.DataContainer.Engine
{
    public sealed partial class DataEngine
    {
        private void ReadRequest(RequestExecutionContext context)
        {
            Serializer.MergeWithLengthPrefix(context.RequestMessage.Stream, context.Request, PrefixStyle.Base128);

            if (context.Request.PrepareOnly)
            {
                // prevent any ambiguities
                context.Request.ReturnDataset = false;
            }

            if (context.Request.HaveParameters)
            {
                context.ParsedRequest.HaveParametersDataInput = true;
                Serializer.MergeWithLengthPrefix(context.RequestMessage.Stream, context.RequestParameters, PrefixStyle.Base128);
                ReadParametersDataInput(context);
            }

            if (context.Request.HaveRequestBulk)
            {
                context.ParsedRequest.IsBulk = true;
                Serializer.MergeWithLengthPrefix(context.RequestMessage.Stream, context.RequestBulk, PrefixStyle.Base128);

                // after this point in stream, bulk input data will be read by instance of InputDataStreamEnumerator 
                // e.g. we're not yet done reading the request stream here
            }

            if (_tracer.IsInfoEnabled)
            {
                var cmdText = context.Request.CommandText;
                if (string.IsNullOrEmpty(cmdText) && context.Request.HaveRequestBulk)
                {
                    cmdText = string.Format("Bulk {0} with {2} items on {1}", context.RequestBulk.DbStatementType, context.RequestBulk.EntityName, context.RequestBulk.InputItemsCount);
                }

                _tracer.Info("Received command: " + cmdText);
            }

            // bring up cache record
            var cacheKey = ParsedRequestCache.GetRequestHash(context.Request, context.RequestBulk, context.RequestParameters);
            var cacheInfo = _parsedRequestCache.AddOrGetExisting(cacheKey, context.Request.HaveParameters);
            context.AttachCachedInfo(cacheInfo);

            // populate cache record
            if (!cacheInfo.HaveRequestHeaders)
            {
                lock (cacheInfo)
                {
                    cacheInfo.CheckIsError();
                    if (!cacheInfo.HaveRequestHeaders)
                    {
                        cacheInfo.ReadRequestHeaders(context.Request, context.RequestParameters, context.RequestBulk, context.ParsedRequest);
                    }
                }
            }
        }

        private void ParseRequest(DataRequest request, DataRequestBulk requestBulk, ParsedRequest parsedRequest, CancellationToken cancellation)
        {
            if (parsedRequest.SpecialCommand.IsSpecialCommand)
            {
                if (parsedRequest.SpecialCommand.CommandType != ParsedRequest.SpecialCommandData.SpecialCommandType.Defragment)
                {
                    throw new CompilationException(
                        "Invalid special command type: " + parsedRequest.SpecialCommand.CommandType + ". Command text was: " + request.CommandText);
                }

                return;
            }

            // by now, we have all information about the request at hand, including parameter values data.
            // parser will write results into cacheInfo object
            _parser.Parse(request, requestBulk, parsedRequest, cancellation);

            if (parsedRequest.StatementType is StatementType.Delete or StatementType.Insert)
            {
                // for insert and delete, we need all columns' data ready for modification before we start
                _storageDriver.PrepareAllColumnsAndWait(parsedRequest.TargetEntity.DocumentType);
            }
            else
            {
                // for other statement types, we only need fields that we order by, filter on, fetch or update,
                // and we need them in that particular order.

                // schedule loading of sort order fields
                foreach (var field in parsedRequest.BaseDataset.OrderClauseFields)
                {
                    _storageDriver.BeginPrepareColumnData(field.Item1);
                }

                // schedule loading of where clause fields
                foreach (var field in parsedRequest.BaseDataset.WhereClauseFields)
                {
                    _storageDriver.BeginPrepareColumnData(field.FieldId);
                }

                // schedule loading of fetched fields
                foreach (var field in parsedRequest.Select.SelectFields)
                {
                    _storageDriver.BeginPrepareColumnData(field.FieldId);
                }

                // schedule loading of inserted/updated fields
                foreach (var field in parsedRequest.Modify.ModifiedFields)
                {
                    _storageDriver.BeginPrepareColumnData(field.FieldId);
                }
            }
        }

        private void ReadParametersDataInput(RequestExecutionContext context)
        {
            var headers = context.RequestParameters;
            var parsed = context.ParsedRequest;
            var paramCount = headers.DataTypes.Length;

            if (headers.IsCollectionFlags.Length != BitVector.GetArrayLength(paramCount))
            {
                throw new Exception(string.Format("BitVector for isCollection flags is broken"));
            }

            parsed.Params.Names = headers.Names;
            parsed.Params.DataTypes = headers.DataTypes;
            parsed.Params.OrdinalToLocalOrdinal = new int[headers.DataTypes.Length];
            var collectionCount = 0;
            var valueCount = 0;
            for (var ordinal = 0; ordinal < paramCount; ordinal++)
            {
                var flag = BitVector.Get(headers.IsCollectionFlags, ordinal);
                if (flag)
                {
                    parsed.Params.OrdinalToLocalOrdinal[ordinal] = collectionCount;
                    collectionCount++;
                }
                else
                {
                    parsed.Params.OrdinalToLocalOrdinal[ordinal] = valueCount;
                    valueCount++;
                }
            }

            // collections are stored separately
            if (collectionCount > 0)
            {
                parsed.Params.InputCollections = new object[collectionCount];
            }

            // single values are stored in an instance of DriverRowData
            if (valueCount > 0)
            {
                var fieldTypesForValues = new DbType[valueCount];
                for (var ordinal = 0; ordinal < headers.DataTypes.Length; ordinal++)
                {
                    if (!BitVector.Get(headers.IsCollectionFlags, ordinal))
                    {
                        fieldTypesForValues[parsed.Params.OrdinalToLocalOrdinal[ordinal]] = headers.DataTypes[ordinal];
                    }
                }

                parsed.Params.InputValues = new DriverRowData(fieldTypesForValues);
            }

            parsed.Bulk.Attach(context.RequestMessage.Stream);
            try
            {
                using (var reader = new BinaryReader(parsed.Bulk, Encoding.UTF8, true))
                {
                    StringBuilder stringBuilder = null;

                    var notnulls = new int[BitVector.GetArrayLength(paramCount)];
                    BitVector.Read(notnulls, paramCount, reader);

                    for (var ordinal = 0; ordinal < paramCount; ordinal++)
                    {
                        var iscollection = BitVector.Get(headers.IsCollectionFlags, ordinal);
                        if (BitVector.Get(notnulls, ordinal))
                        {
                            var dbType = headers.DataTypes[ordinal];

                            if (stringBuilder == null
                                && RowData.DeriveSystemType(dbType) == typeof (string))
                            {
                                stringBuilder = new StringBuilder();
                            }

                            // we have more than one destination, each storing a subset of input values
                            // so ordinals are different from "flat" zero-to-paramCount enumeration
                            var localOrdinal = parsed.Params.OrdinalToLocalOrdinal[ordinal];

                            if (iscollection)
                            {
                                parsed.Params.InputCollections[localOrdinal] = ReadCollection(dbType, reader, stringBuilder);
                            }
                            else
                            {
                                BitVector.Set(parsed.Params.InputValues.NotNulls, localOrdinal);
                                ReadPrimitiveValue(parsed.Params.InputValues, localOrdinal, reader, stringBuilder);
                            }
                        }
                    }
                }

                // client sets stream end marker after parameters data, make sure we read it
                // otherwise subsequent bulk reader may fail
                if (-1 != parsed.Bulk.ReadByte())
                {
                    throw new Exception("Did not find the end of parameters data block when expected");
                }
            }
            finally
            {
                parsed.Bulk.Detach();
            }
        }

        private object ReadCollection(DbType dbType, BinaryReader reader, StringBuilder stringBuilder)
        {
            var itemCount = RowData.Read7BitEncodedInt(reader);

            switch (dbType)
            {
                //case DbType.VarNumeric:
                //    break;
                case DbType.AnsiString:
                case DbType.String:
                case DbType.AnsiStringFixedLength:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    {
                        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                        for (var x = 0; x < itemCount; x++)
                        {
                            var len = RowData.Read7BitEncodedInt(reader);
                            if (len > 0)
                            {
                                stringBuilder.Clear();
                                stringBuilder.EnsureCapacity(len);
                                for (var i = 0; i < len; i++)
                                {
                                    stringBuilder.Append((char) RowData.Read7BitEncodedInt(reader));
                                }
                                result.Add(stringBuilder.ToString());
                            }
                            else if (len >= -1)
                            {
                                // client supplies -1 to indicate that collection element is null
                                // ignore ReSharper's warning about notnull, no exception here

                                // ReSharper disable AssignNullToNotNullAttribute
                                result.Add(len == 0 ? string.Empty : null);
                                // ReSharper restore AssignNullToNotNullAttribute
                            }
                            else
                            {
                                throw new Exception("Invalid length value: " + len);
                            }
                        }
                        return result;
                    }

                case DbType.Binary:
                case DbType.Object:
                    {
                        var result = new HashSet<SizableArrayOfByte>(SizableArrayOfByte.DefaultComparer.Instance);

                        for (var x = 0; x < itemCount; x++)
                        {
                            var len = RowData.Read7BitEncodedInt(reader);
                            if (len >= 0)
                            {
                                var data = new SizableArrayOfByte();
                                data.SetLength(len);
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

                                result.Add(data);
                            }
                            else if (len == -1)
                            {
                                // client supplies -1 to indicate that collection element is null
                                // ignore ReSharper's warning about notnull, no exception here

                                // ReSharper disable AssignNullToNotNullAttribute
                                result.Add(null);
                                // ReSharper restore AssignNullToNotNullAttribute
                            }
                            else
                            {
                                throw new Exception("Invalid length value: " + len);
                            }
                        }
                        return result;
                    }

                case DbType.SByte:
                    {
                        var result = new HashSet<sbyte>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add((sbyte)reader.ReadByte());
                        }
                        return result;
                    }

                case DbType.Byte:
                    {
                        var result = new HashSet<byte>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadByte());
                        }
                        return result;
                    }
                    
                case DbType.Boolean:
                    {
                        var result = new HashSet<bool>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadBoolean());
                        }
                        return result;
                    }
                    
                case DbType.Decimal:
                case DbType.Currency:
                    {
                        var result = new HashSet<decimal>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadDecimal());
                        }
                        return result;
                    }
                    
                case DbType.Guid:
                    {
                        var result = new HashSet<Guid>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            var buf = new DriverRowData.ValueHolder16Bytes { Lo = reader.ReadInt64(), Hi = reader.ReadInt64() };
                            result.Add(buf.AsGuid);
                        }
                        return result;
                    }
                    
                case DbType.DateTimeOffset:
                    {
                        var result = new HashSet<DateTimeOffset>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            var buf = new DriverRowData.ValueHolder16Bytes {Lo = reader.ReadInt64(), Hi = reader.ReadInt64()};
                            result.Add(buf.AsDateTimeOffset);
                        }
                        return result;
                    }
                    
                case DbType.Int16:
                    {
                        var result = new HashSet<short>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadInt16());
                        }
                        return result;
                    }
                    
                case DbType.UInt16:
                    {
                        var result = new HashSet<ushort>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadUInt16());
                        }
                        return result;
                    }
                    
                case DbType.Int32:
                    {
                        var result = new HashSet<int>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadInt32());
                        }
                        return result;
                    }
                    
                case DbType.UInt32:
                    {
                        var result = new HashSet<uint>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadUInt32());
                        }
                        return result;
                    }
                    
                case DbType.Single:
                    {
                        var result = new HashSet<float>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadSingle());
                        }
                        return result;
                    }
                    
                case DbType.Date:
                case DbType.DateTime:
                case DbType.DateTime2:
                    {
                        var result = new HashSet<DateTime>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(DateTime.FromBinary(reader.ReadInt64()));
                        }
                        return result;
                    }
                case DbType.Time:
                    {
                        var result = new HashSet<TimeSpan>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(new TimeSpan(reader.ReadInt64()));
                        }
                        return result;
                    }
                    
                case DbType.Int64:
                    {
                        var result = new HashSet<long>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadInt64());
                        }
                        return result;
                    }
                    
                case DbType.UInt64:
                    {
                        var result = new HashSet<ulong>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadUInt64());
                        }
                        return result;
                    }
                    
                case DbType.Double:
                    {
                        var result = new HashSet<double>();
                        for (var x = 0; x < itemCount; x++)
                        {
                            result.Add(reader.ReadDouble());
                        }
                        return result;
                    }
                    
                default:
                    throw new DataException("Invalid DbType: " + dbType);
            }
        }

        private void ReadPrimitiveValue(DriverRowData rowData, int ordinal, BinaryReader reader, StringBuilder stringBuilder)
        {
            var indexInArray = rowData.FieldArrayIndexes[ordinal];

            switch (rowData.FieldTypes[ordinal])
            {
                //case DbType.VarNumeric:
                //    break;
                case DbType.AnsiString:
                case DbType.String:
                case DbType.AnsiStringFixedLength:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    {
                        var len = RowData.Read7BitEncodedInt(reader);
                        if (len > 0)
                        {
                            stringBuilder.Clear();
                            stringBuilder.EnsureCapacity(len);
                            for (var i = 0; i < len; i++)
                            {
                                stringBuilder.Append((char) RowData.Read7BitEncodedInt(reader));
                            }
                            rowData.StringData[indexInArray] = stringBuilder.ToString();
                        }
                        else
                        {
                            rowData.StringData[indexInArray] = string.Empty;
                        }
                    }
                    break;

                case DbType.Binary:
                case DbType.Object:
                    {
                        var data = rowData.BinaryData[indexInArray];
                        data.SetLength(RowData.Read7BitEncodedInt(reader));
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
                    rowData.ValueData8Bytes[indexInArray].AsByte = reader.ReadByte();
                    break;
                case DbType.Boolean:
                    rowData.ValueData8Bytes[indexInArray].AsBoolean = reader.ReadBoolean();
                    break;
                case DbType.Decimal:
                case DbType.Currency:
                case DbType.Guid:
                case DbType.DateTimeOffset:
                    rowData.ValueData16Bytes[indexInArray].Lo = reader.ReadInt64();
                    rowData.ValueData16Bytes[indexInArray].Hi = reader.ReadInt64();
                    break;
                case DbType.Int16:
                case DbType.UInt16:
                    rowData.ValueData8Bytes[indexInArray].AsInt16 = reader.ReadInt16();
                    break;
                case DbType.Int32:
                case DbType.UInt32:
                case DbType.Single:
                    rowData.ValueData8Bytes[indexInArray].AsInt32 = reader.ReadInt32();
                    break;
                case DbType.Date:
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.Time:
                case DbType.Int64:
                case DbType.UInt64:
                case DbType.Double:
                    rowData.ValueData8Bytes[indexInArray].AsInt64 = reader.ReadInt64();
                    break;

                default:
                    throw new DataException("Invalid DbType: " + rowData.FieldTypes[ordinal]);
            }
        }
    }
}