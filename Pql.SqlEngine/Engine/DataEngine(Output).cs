using System;
using System.Data;
using System.IO;
using Pql.ClientDriver.Protocol;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;
using Pql.ExpressionEngine.Interfaces;

namespace Pql.Engine.DataContainer.Engine
{
    public sealed partial class DataEngine
    {
        private bool WriteItems(
            RequestExecutionContext context, RequestExecutionBuffer buffer, IDriverDataEnumerator sourceEnumerator, bool havePendingDriverRow)
        {
            // have to check for disposal here, because this method gets invoked in a loop
            if (m_disposed)
            {
                buffer.Error = new ObjectDisposedException("This data engine has been disposed");
                return false;
            }

            if (sourceEnumerator == null)
            {
                return false;
            }

            var stream = buffer.Stream;
            var writer = buffer.Writer;
            var cts = context.CancellationTokenSource;

            buffer.RowsOutput = 0;

            var hasPendingWrite = false;
            var mustReturnDataset = context.Request.ReturnDataset;
            var lastValidLength = stream.Length;
            var totalRowsProduced = context.TotalRowsProduced;
            var recordsAffected = context.RecordsAffected;
            var rowsOutputLocally = 0;

            var func = context.ParsedRequest.BaseDataset.Paging.Offset;
            var pagingOffset = func is null ? 0 : func(context.ParsedRequest.Params.InputValues);

            func = context.ParsedRequest.BaseDataset.Paging.PageSize;
            var pageSize = func is null ? Int32.MaxValue : func(context.ParsedRequest.Params.InputValues);

            // one row might not have fit into previous buffer, let's write it now
            if (havePendingDriverRow)
            {
                try
                {
                    // no need to apply paging and where clause for pending row: we applied them already
                    // also expect that a buffer can accomodate at least one row 
                    // - this requires that rows are never larger than RequestExecutionBuffer.MaxBytesPerBuffer
                    ProduceOutputRow(context);
                    context.OutputDataBuffer.Write(writer);

                    rowsOutputLocally++;
                    lastValidLength = stream.Length;
                }
                catch (Exception e)
                {
                    throw new Exception("Internal buffer may be too small to fit even a single data row", e);
                }
            }

            // now let's deal with remaining items in the enumerator
            while (lastValidLength < RequestExecutionBuffer.MaxBytesPerBuffer && !cts.IsCancellationRequested)
            {
                if (recordsAffected >= pageSize || !sourceEnumerator.MoveNext())
                {
                    // enough rows accumulated, or no more rows from driver: halt
                    break;
                }

                // if record satisfies WHERE criteria, it gets counted into pre-paging total
                if (ApplyWhereClause(context))
                {
                    var isAccumulating = totalRowsProduced >= pagingOffset;

                    totalRowsProduced++;

                    if (isAccumulating)
                    {
                        // once WHERE has been evaluated, read all remaining fields for this row into the buffer
                        sourceEnumerator.FetchAdditionalFields();

                        // output row number is needed for "rownumoutput()" Pql function
                        context.ClauseEvaluationContext.RowNumberInOutput = recordsAffected;

                        // increment counter BEFORE producing and writing
                        // even if we fail to write into current buffer, we'll write into next one
                        recordsAffected++;

                        // this will be false when clients do ExecuteNonQuery or Prepare
                        if (mustReturnDataset)
                        {
                            // produce SELECT (output scheme) from FROM (raw data from storage driver)
                            var estimatedSize = ProduceOutputRow(context);

                            if (lastValidLength + estimatedSize > RequestExecutionBuffer.MaxBytesPerBuffer)
                            {
                                // store pending write and return
                                hasPendingWrite = true;
                                break;
                            }

                            // MemoryStream will throw NotSupportedException when trying to expand beyond fixed buffer size
                            // this should never happen (see check above)
                            context.OutputDataBuffer.Write(writer);

                            // this counter gets incremented AFTER writing, 
                            // because it indicates how many rows have in fact been put into current block
                            rowsOutputLocally++;
                        }

                        lastValidLength = stream.Length;
                    }

                    // row number is needed for "rownum()" Pql function
                    context.ClauseEvaluationContext.RowNumber++;
                }
            }

            buffer.RowsOutput = rowsOutputLocally;
            context.RecordsAffected = recordsAffected;
            context.TotalRowsProduced = totalRowsProduced;

            stream.Seek(0, SeekOrigin.Begin);
            return hasPendingWrite;
        }

        /// <summary>
        /// Takes driver output row and produces SELECt output row.
        /// Returns ESTIMATED upper bound for byte size of the new SELECT output row.
        /// </summary>
        private static int ProduceOutputRow(RequestExecutionContext context)
        {
            var output = context.OutputDataBuffer;
            var outputSize = output.GetMinimumSize();
            var ctx = context.ClauseEvaluationContext;

            BitVector.SetAll(output.NotNulls, false);

            for (var ordinal = 0; ordinal < context.ResponseHeaders.FieldCount; ordinal++)
            {
                var indexInArray = output.FieldArrayIndexes[ordinal];

                var isNullable = context.ParsedRequest.Select.OutputColumns[ordinal].IsNullable;
                var compiledExpression = context.ParsedRequest.Select.OutputColumns[ordinal].CompiledExpression;

                switch (output.FieldTypes[ordinal])
                {
                    //case DbType.VarNumeric:
                    //    break;
                    case DbType.AnsiStringFixedLength:
                    case DbType.StringFixedLength:
                    case DbType.Xml:
                    case DbType.AnsiString:
                    case DbType.String:
                        {
                            var value = ((Func<ClauseEvaluationContext, string>)compiledExpression)(ctx);
                            if (value != null)
                            {
                                output.StringData[indexInArray].SetLength(value.Length);
                                for (var i = 0; i < value.Length; i++)
                                {
                                    output.StringData[indexInArray].Data[i] = value[i];
                                }

                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(int) + value.Length * sizeof(char);
                            }
                        }
                        break;
                    case DbType.Binary:
                    case DbType.Object:
                        {
                            var data = ((Func<ClauseEvaluationContext, SizableArrayOfByte>)compiledExpression)(ctx);
                            if (data != null)
                            {
                                var len = data.Length;
                                output.BinaryData[indexInArray].SetLength(len);
                                if (len > 0)
                                {
                                    Buffer.BlockCopy(data.Data, 0, output.BinaryData[indexInArray].Data, 0, len);
                                }
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(int) + data.Length;
                            }
                        }
                        break;
                    case DbType.Byte:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Byte>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsByte = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize++;
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Byte>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsByte = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize++;
                            }
                        }
                        break;
                    case DbType.Boolean:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Boolean>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsBoolean = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize++;
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Boolean>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsBoolean = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize++;
                            }
                        }
                        break;
                    case DbType.Currency:
                    case DbType.Decimal:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Decimal>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData16Bytes[indexInArray].AsDecimal = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Decimal);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Decimal>)compiledExpression)(ctx);
                                output.ValueData16Bytes[indexInArray].AsDecimal = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Decimal);
                            }
                        }
                        break;
                    case DbType.Date:
                    case DbType.DateTime:
                    case DbType.DateTime2:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<DateTime>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsDateTime = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Int64);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, DateTime>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsDateTime = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Int64);
                            }
                        }
                        break;
                    case DbType.Time:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<TimeSpan>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsTimeSpan = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Int64);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, TimeSpan>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsTimeSpan = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Int64);
                            }
                        }
                        break;
                    case DbType.Double:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Double>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsDouble = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Double);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Double>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsDouble = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Double);
                            }
                        }
                        break;
                    case DbType.Guid:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Guid>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData16Bytes[indexInArray].AsGuid = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += 16;
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Guid>)compiledExpression)(ctx);
                                output.ValueData16Bytes[indexInArray].AsGuid = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += 16;
                            }
                        }
                        break;
                    case DbType.Int16:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Int16>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsInt16 = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Int16);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Int16>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsInt16 = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Int16);
                            }
                        }
                        break;
                    case DbType.Int32:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Int32>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsInt32 = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Int32);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Int32>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsInt32 = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Int32);
                            }
                        }
                        break;
                    case DbType.Int64:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Int64>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsInt64 = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Int64);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Int64>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsInt64 = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Int64);
                            }
                        }
                        break;
                    case DbType.SByte:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<SByte>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsSByte = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize++;
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, SByte>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsSByte = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize++;
                            }
                        }
                        break;
                    case DbType.Single:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<Single>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsSingle = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(Single);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, Single>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsSingle = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(Single);
                            }
                        }
                        break;
                    case DbType.UInt16:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<UInt16>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsUInt16 = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(UInt16);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, UInt16>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsUInt16 = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(UInt16);
                            }
                        }
                        break;
                    case DbType.UInt32:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<UInt32>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsUInt32 = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(UInt32);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, UInt32>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsUInt32 = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(UInt32);
                            }
                        }
                        break;
                    case DbType.UInt64:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<UInt64>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData8Bytes[indexInArray].AsUInt64 = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += sizeof(UInt64);
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, UInt64>)compiledExpression)(ctx);
                                output.ValueData8Bytes[indexInArray].AsUInt64 = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += sizeof(UInt64);
                            }
                        }
                        break;
                    case DbType.DateTimeOffset:
                        {
                            if (isNullable)
                            {
                                var value = ((Func<ClauseEvaluationContext, UnboxableNullable<DateTimeOffset>>)compiledExpression)(ctx);
                                if (value.HasValue)
                                {
                                    output.ValueData16Bytes[indexInArray].AsDateTimeOffset = value.Value;
                                    BitVector.Set(output.NotNulls, ordinal);
                                    outputSize += 12;
                                }
                            }
                            else
                            {
                                var value = ((Func<ClauseEvaluationContext, DateTimeOffset>)compiledExpression)(ctx);
                                output.ValueData16Bytes[indexInArray].AsDateTimeOffset = value;
                                BitVector.Set(output.NotNulls, ordinal);
                                outputSize += 12;
                            }
                        }
                        break;
                    default:
                        throw new Exception("Invalid data type: " + output.FieldTypes[ordinal]);
                }
            }

            return outputSize;
        }
    }
}