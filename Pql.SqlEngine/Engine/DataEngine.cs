using System;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Pql.ClientDriver;
using Pql.ClientDriver.Protocol;
using Pql.Engine.DataContainer.Parser;
using Pql.Engine.Interfaces;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;
using ProtoBuf;

namespace Pql.Engine.DataContainer.Engine
{
    public sealed partial class DataEngine : IDataEngine
    {
        private readonly ConcurrentDictionary<RequestExecutionContext, Task> m_activeProcessors;
        private readonly int m_maxConcurrency;
        private readonly ITracer m_tracer;
        private readonly ParsedRequestCache m_parsedRequestCache;
        private readonly IStorageDriver m_storageDriver;
        private readonly QueryParser m_parser;
        private readonly DataContainerDescriptor m_containerDescriptor;
        private DateTime m_utcLastUsedAt;
        private volatile bool m_disposed;

        public DataEngine(ITracer tracer, string instanceName, int maxConcurrency, IStorageDriver storageDriver, DataContainerDescriptor containerDescriptor)
        {
            if (maxConcurrency <= 0 || maxConcurrency > 10000)
            {
                throw new ArgumentOutOfRangeException(nameof(maxConcurrency));
            }

            m_tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
            m_containerDescriptor = containerDescriptor ?? throw new ArgumentNullException(nameof(containerDescriptor));
            m_maxConcurrency = maxConcurrency;
            m_parsedRequestCache = new ParsedRequestCache(instanceName);
            m_storageDriver = storageDriver ?? throw new ArgumentNullException(nameof(storageDriver));
            m_parser = new QueryParser(containerDescriptor, maxConcurrency);
            m_activeProcessors = new ConcurrentDictionary<RequestExecutionContext, Task>(m_maxConcurrency, m_maxConcurrency);
            m_utcLastUsedAt = DateTime.UtcNow;
        }

        public DateTime UtcLastUsedAt
        {
            get { return m_activeProcessors.Count > 0 ? DateTime.UtcNow : m_utcLastUsedAt; }
        }

        public void WriteStateInfoToLog()
        {
            if (m_disposed || !m_tracer.IsInfoEnabled)
            {
                return;
            }

            var lastUsed = UtcLastUsedAt;
            m_tracer.InfoFormat("Number of active contexts: {0}, Last used: {1}, Unused age: {2}", 
                m_activeProcessors.Count, lastUsed, (DateTime.UtcNow - lastUsed));
            foreach (var item in m_activeProcessors)
            {
                try
                {
                    var ctx = item.Key;
                    var ageSeconds = (DateTime.Now - ctx.RequestMessage.CreatedOn).TotalSeconds;
                    var cmdText = ctx.Request.CommandText;
                    if (string.IsNullOrEmpty(cmdText) && ctx.Request.HaveRequestBulk)
                    {
                        cmdText = string.Format("Bulk {0} with {2} items on {1}", ctx.RequestBulk.DbStatementType, ctx.RequestBulk.EntityName, ctx.RequestBulk.InputItemsCount);
                    }

                    m_tracer.InfoFormat(
                        "AuthTicket: {0}; CommandText: {1}; PrepareOnly: {2}; ReturnDataset: {3}; HaveRequestBulk: {4}; HaveParams: {5}; "
                        + "TotalRowsProduced: {6}; records affected: {7}; received on: {8}; age: {9} seconds; producer status: {10}", 
                        ctx.RequestMessage.AuthTicket, cmdText, ctx.Request.PrepareOnly, ctx.Request.ReturnDataset, 
                        ctx.Request.HaveRequestBulk, ctx.Request.HaveParameters,
                        ctx.TotalRowsProduced, ctx.RecordsAffected, ctx.RequestMessage.CreatedOn.ToString("s"), 
                        ageSeconds, item.Value.Status);
                }
                catch {}
            }
        }

        public void Dispose()
        {
            if (Environment.HasShutdownStarted)
            {
                return;
            }

            m_disposed = true;

            foreach (var executionContext in m_activeProcessors.Keys)
            {
                try
                {
                    new Task(state => ((RequestExecutionContext) state).Cancel(null), executionContext).Start();
                }
                catch (Exception e)
                {
                    if (Environment.HasShutdownStarted)
                    {
                        return;
                    }

                    m_tracer.Exception(e);
                }
            }
            
            m_parsedRequestCache.Dispose();
            
            if (m_storageDriver != null)
            {
                m_storageDriver.Dispose();
            }
        }

        public void BeginExecution(RequestExecutionContext context)
        {
            CheckDisposed();

            m_utcLastUsedAt = DateTime.UtcNow;

            if (m_activeProcessors.ContainsKey(context))
            {
                throw new InvalidOperationException("This context is marked as being executed now");
            }

            var task = new Task(
                () => ProducerThreadMethod(context),
                TaskCreationOptions.LongRunning);

            if (!m_activeProcessors.TryAdd(context, task))
            {
                throw new Exception("Could not register executing task for this context");
            }

            try
            {
                task.Start();
            }
            catch
            {
                m_activeProcessors.TryRemove(context, out task);
                throw;
            }
        }

        /// <summary>
        /// <see cref="ProducerThreadMethod"/> works in parallel with RPM's <see cref="RequestProcessingManager.WriteTo"/>.
        /// RPM supplies empty buffers to be filled with data into <see cref="RequestExecutionContext.BuffersRing"/> and consumes them on the other end.        
        /// The data ring has very limited number of buffers.
        /// RPM is limited by network throughput and Producer's speed.
        /// Producer is limited by underlying storage driver, local processing speed and RPM's consumption of complete buffers.
        /// The difference between the two: RPM <see cref="RequestProcessingManager.WriteTo"/> is scheduled for execution by service infrastructure (WCF),
        /// whereas <see cref="DataEngine.ProducerThreadMethod"/> is scheduled by RPM itself, when it invokes <see cref="IDataEngine.BeginExecution"/>.
        /// </summary>
        void ProducerThreadMethod(RequestExecutionContext context)
        {
            PqlEngineSecurityContext.Set(new PqlClientSecurityContext(
                    context.AuthContext.UserId, "dummy", context.AuthContext.TenantId, context.AuthContext.ContextId));

            var executionPending = true;
            IDriverDataEnumerator sourceEnumerator = null;
            try
            {
                // row number is needed for "rownum()" Pql function
                context.ClauseEvaluationContext.RowNumber = 0;

                // Our production is limited by the network throughput.
                // Production will also be aborted if the destination sink stops accepting.
                // In that case, ConsumingEnumerable will either throw or stop yielding.
                bool havePendingDriverRow = false;
                foreach (var buffer in context.BuffersRing.ConsumeProcessingTasks(context.CancellationTokenSource.Token)
                    )
                {
                    buffer.Cleanup();

                    try
                    {
                        if (executionPending)
                        {
                            executionPending = false;

                            // read network protocol message
                            // parse-compile expressions and collect information for execution plan
                            // generate response headers and fetch data from Redis
                            // this place fails most often, because of Pql compilation or storage driver connectivity failures
                            StartProduction(context, buffer, out sourceEnumerator);

                            if (context.Request.ReturnDataset)
                            {
                                // write response headers BEFORE the query processing is completed
                                // records affected and whatever other stats will be zero
                                Serializer.SerializeWithLengthPrefix(buffer.Stream, context.ResponseHeaders, PrefixStyle.Base128);
                            }
                        }

                        // go through retrieved data
                        havePendingDriverRow = ((DataEngine) context.Engine).WriteItems(
                            context, buffer, sourceEnumerator, havePendingDriverRow);

                        // some consistency checks
                        if (context.Request.ReturnDataset)
                        {
                            if (havePendingDriverRow && buffer.RowsOutput == 0)
                            {
                                throw new Exception("Internal error: should not have pending row when no data is produced");
                            }
                        }
                        else
                        {
                            if (havePendingDriverRow)
                            {
                                throw new Exception("Internal error: should not have pending row when no dataset is requested");
                            }

                            if (buffer.Stream.Length > 0)
                            {
                                throw new Exception("Internal error: should not have written anything to stream when no dataset is requested");
                            }
                        }

                        // time to quit? 
                        // no dataset requested, don't have any more data, or enough rows accumulated for requested page of results?
                        if (buffer.RowsOutput == 0 || buffer.IsFailed || !context.Request.ReturnDataset)
                        {
                            if (!context.Request.ReturnDataset)
                            {
                                // if there is no dataset sent, write response headers AFTER processing the query
                                // records affected and whatever other stats are meaningful
                                context.ResponseHeaders.RecordsAffected = context.RecordsAffected;
                                Serializer.SerializeWithLengthPrefix(buffer.Stream, context.ResponseHeaders, PrefixStyle.Base128);
                            }

                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        buffer.Error = e;
                        context.TrySetLastError(e);
                        m_tracer.Exception(e);

                        // this will go to client, and overwrite whatever we managed to put into buffer before failure
                        using var writer = new PqlErrorDataWriter(1, e, false);
                        buffer.Stream.SetLength(0);
                        writer.WriteTo(buffer.Stream);

                        return;
                    }
                    finally
                    {
                        // return the buffer back to the ring in any case
                        context.BuffersRing.ReturnCompletedTask(buffer);
                    }
                }
            }
            catch (OperationCanceledException e)
            {
                context.Cancel(e);
            }
            catch (Exception e)
            {
                if (Environment.HasShutdownStarted)
                {
                    // nobody cares now
                    return;
                }

                var cts = context.CancellationTokenSource;
                if (cts != null && !cts.IsCancellationRequested)
                {
                    m_tracer.Exception(e);
                    context.Cancel(e);
                }
            }
            finally
            {
                var ring = context.BuffersRing;
                if (ring != null)
                {
                    ring.CompleteAddingCompletedTasks();
                }

                if (sourceEnumerator != null)
                {
                    // release driver-level resources & locks
                    sourceEnumerator.Dispose();
                }
            }
        }

        private void StartProduction(
            RequestExecutionContext context, RequestExecutionBuffer buffer, out IDriverDataEnumerator sourceEnumerator)
        {
            ReadRequest(context);

            context.AttachContainerDescriptor(m_containerDescriptor);

            if (!context.CacheInfo.HaveParsingResults)
            {
                lock (context.CacheInfo)
                {
                    context.CacheInfo.CheckIsError();
                    if (!context.CacheInfo.HaveParsingResults)
                    {
                        try
                        {
                            ParseRequest(context.Request, context.RequestBulk, context.CacheInfo.ParsedRequest, context.CancellationTokenSource.Token);
                            CompileClauses(context.ContainerDescriptor, context.CacheInfo);

                            Thread.MemoryBarrier();
                            context.CacheInfo.HaveParsingResults = true;
                        }
                        catch (Exception e)
                        {
                            // make sure that partially complete results do not become visible
                            context.CacheInfo.IsError(e);
                            throw;
                        }
                    }
                }
            }

            context.CacheInfo.WriteParsingResults(context.ParsedRequest);

            if (context.ParsedRequest.SpecialCommand.IsSpecialCommand)
            {
                sourceEnumerator = null;
                ExecuteSpecialCommandStatement(context, buffer);
                return;
            }

            // structure of output buffer depends on which fields client is asking for
            // therefore, we re-create and re-attach a driver output buffer for every request
            context.AttachDriverOutputBufferAndInputParameters(
                QueryParser.CreateDriverRowDataBuffer(context.ParsedRequest.BaseDataset.BaseFields),
                context.ParsedRequest);

            // this enumerator will yield our own driverOutputBuffer for every source record
            // e.g. the very same context.DriverOutputBuffer is going to be yielded N times from this enumerator 
            if (context.ParsedRequest.StatementType == StatementType.Insert)
            {
                if (context.ParsedRequest.IsBulk)
                {
                    sourceEnumerator = CreateInputDataEnumerator(context);
                    //m_storageDriver.AllocateCapacityForDocumentType(context.ParsedRequest.TargetEntity.DocumentType, context.RequestBulk.InputItemsCount);
                }
                else
                {
                    sourceEnumerator = CreatePseudoEnumeratorForInsertValues(context);
                }
            }
            else
            {
                if (context.ParsedRequest.IsBulk)
                {
                    // for SELECT and DELETE, we only use PK values from the input enumerator
                    // for UPDATE, we use both PK values and other field values from input enumerator
                    context.AttachInputDataEnumerator(CreateInputDataEnumerator(context));
                }

                // driver returns set of rows related to given set of PK values
                // for a bulk request, sourceEnumerator will yield exactly one item for each item in input enumerator
                sourceEnumerator = m_storageDriver.GetData(context);
            }

            switch (context.ParsedRequest.StatementType)
            {
                case StatementType.Select:
                    {
                        context.AttachResponseHeaders(CreateResponseSchemeForSelect(context));
                        context.PrepareBuffersForSelect();
                        context.ResponseHeaders.RecordsAffected = 0;
                    }
                    break;
                case StatementType.Update:
                    {
                        context.AttachResponseHeaders(new DataResponse(0, "Update successful"));
                        context.PrepareBuffersForUpdate();
                        ExecuteInsertUpdateStatement(context, buffer, sourceEnumerator, DriverChangeType.Update);
                        context.ResponseHeaders.RecordsAffected = context.RecordsAffected;
                    }
                    break;
                case StatementType.Delete:
                    {
                        context.AttachResponseHeaders(new DataResponse(0, "Delete successful"));
                        context.PrepareBuffersForDelete();
                        ExecuteDeleteStatement(context, buffer, sourceEnumerator);
                        context.ResponseHeaders.RecordsAffected = context.RecordsAffected;
                    }
                    break;
                case StatementType.Insert:
                    {
                        context.AttachResponseHeaders(new DataResponse(0, "Insert successful"));
                        context.PrepareChangeBufferForInsert();
                        ExecuteInsertUpdateStatement(context, buffer, sourceEnumerator, DriverChangeType.Insert);
                        context.ResponseHeaders.RecordsAffected = context.RecordsAffected;
                    }
                    break;
                default:
                    throw new Exception("Invalid statement type: " + context.ParsedRequest.StatementType);
            }
        }

        /// <summary>
        /// Clients can supply a stream of data with bulk requests. 
        /// This input stream may contain values for any fields, but its usage is determined by request type.
        /// For SELECT and DELETE bulk requests, engine will only use values of primary key field.
        /// For INSERT and UPDATE bulk requests, engine will use both PK values and other fields values.
        /// However, iterator will read and parse ALL values in the input stream regardless of which values are used.
        /// </summary>
        private IDriverDataEnumerator CreateInputDataEnumerator(RequestExecutionContext context)
        {
            var types = new DbType[context.ParsedRequest.Modify.InsertUpdateSetClauses.Count];
            for (var i = 0; i < types.Length; i++)
            {
                var field = context.ParsedRequest.Modify.ModifiedFields[i];
                types[i] = field.DbType;
            }

            context.ParsedRequest.Bulk.Attach(context.RequestMessage.Stream);
            
            return new InputDataStreamEnumerator(
                context.RequestBulk.InputItemsCount, types, context.ParsedRequest.Bulk, context.ClauseEvaluationContext.InputRow);
        }

        private IDriverDataEnumerator CreatePseudoEnumeratorForInsertValues(RequestExecutionContext context)
        {
            // generate a single logical entry and break
            // actual values are computed as part of universal insert/update routine
            return new SourcedEnumerator(
                DriverRowData.DeriveRepresentationType(context.ParsedRequest.TargetEntityPkField.DbType));
        }

        private void ExecuteDeleteStatement(RequestExecutionContext context, RequestExecutionBuffer buffer, IDriverDataEnumerator sourceEnumerator)
        {
            if (sourceEnumerator == null)
            {
                return;
            }
            
            buffer.RowsOutput = 0;

            var cts = context.CancellationTokenSource;

            context.ClauseEvaluationContext.RowNumber = 0;
            context.ClauseEvaluationContext.RowNumberInOutput = context.ClauseEvaluationContext.RowNumber;
            var changeBuffer = context.ClauseEvaluationContext.ChangeBuffer;

            var changeset = m_storageDriver.CreateChangeset(changeBuffer, context.ParsedRequest.IsBulk);
            try
            {
                changeBuffer.ChangeType = DriverChangeType.Delete;

                while (!cts.IsCancellationRequested && sourceEnumerator.MoveNext())
                {
                    // if record satisfies WHERE criteria, compute updated values and give them to driver
                    if (ApplyWhereClause(context))
                    {
                        // load internal ID, it is needed 
                        sourceEnumerator.FetchAdditionalFields();
                        sourceEnumerator.FetchInternalEntityIdIntoChangeBuffer(changeBuffer, context);

                        m_storageDriver.AddChange(changeset);
                    }

                    // row number is needed for "rownum()" Pql function
                    context.ClauseEvaluationContext.RowNumber++;
                    // output row number is needed for "rownumoutput()" Pql function
                    context.ClauseEvaluationContext.RowNumberInOutput = context.ClauseEvaluationContext.RowNumber;
                }

                if (!cts.IsCancellationRequested)
                {
                    context.RecordsAffected = m_storageDriver.Apply(changeset);
                }
            }
            catch
            {
                m_storageDriver.Discard(changeset);
                throw;
            }
        }

        private void ExecuteSpecialCommandStatement(RequestExecutionContext context, RequestExecutionBuffer buffer)
        {
            switch (context.ParsedRequest.SpecialCommand.CommandType)
            {
                case ParsedRequest.SpecialCommandData.SpecialCommandType.Defragment:
                    context.AttachResponseHeaders(new DataResponse(0, "Defragmentation completed"));
                    m_storageDriver.Compact(CompactionOptions.FullReindex);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(context), context.ParsedRequest.SpecialCommand.CommandType, "Invalid special command");
            }
        }

        private void ExecuteInsertUpdateStatement(RequestExecutionContext context, RequestExecutionBuffer buffer, IDriverDataEnumerator sourceEnumerator, DriverChangeType changeType)
        {
            if (sourceEnumerator == null)
            {
                return;
            }

            buffer.RowsOutput = 0;
            
            var cts = context.CancellationTokenSource;

            var updates = context.ParsedRequest.Modify.UpdateAssignments;

            context.ClauseEvaluationContext.RowNumber = 0;
            context.ClauseEvaluationContext.RowNumberInOutput = context.ClauseEvaluationContext.RowNumber;
            var changeBuffer = context.ClauseEvaluationContext.ChangeBuffer;
            
            var changeset = m_storageDriver.CreateChangeset(changeBuffer, context.ParsedRequest.IsBulk);
            try
            {
                changeBuffer.ChangeType = changeType;
                
                while (!cts.IsCancellationRequested && sourceEnumerator.MoveNext())
                {
                    // if record satisfies WHERE criteria, compute updated values and give them to driver
                    if (ApplyWhereClause(context))
                    {
                        // make sure we have values for fields in SET expressions
                        sourceEnumerator.FetchAdditionalFields();
                        
                        BitVector.SetAll(changeBuffer.Data.NotNulls, false);
                        for (var ordinal = 0; ordinal < updates.Count; ordinal++)
                        {
                            if (updates[ordinal].CompiledExpression != null)
                            {
                                updates[ordinal].CompiledExpression(context.ClauseEvaluationContext);
                            }
                        }

                        // this will either take internal entity id from current data row
                        // or from the computed change buffer data (for non-bulk inserts)
                        sourceEnumerator.FetchInternalEntityIdIntoChangeBuffer(changeBuffer, context);
                        
                        m_storageDriver.AddChange(changeset);
                    }

                    // row number is needed for "rownum()" Pql function
                    context.ClauseEvaluationContext.RowNumber++;
                    // output row number is needed for "rownumoutput()" Pql function
                    context.ClauseEvaluationContext.RowNumberInOutput = context.ClauseEvaluationContext.RowNumber;
                }

                if (!cts.IsCancellationRequested)
                {
                    context.RecordsAffected = m_storageDriver.Apply(changeset);
                }
            }
            catch
            {
                m_storageDriver.Discard(changeset);
                throw;
            }
        }

        private static bool ApplyWhereClause(RequestExecutionContext context)
        {
            return context.ParsedRequest.BaseDataset.WhereClauseProcessor == null 
                || context.ParsedRequest.BaseDataset.WhereClauseProcessor(context.ClauseEvaluationContext);
        }

        public void EndExecution(RequestExecutionContext context, bool waitForProducerThread)
        {
            if (m_disposed)
            {
                return;
            }

            var processors = m_activeProcessors;
            if (processors != null)
            {
                if (processors.TryRemove(context, out var task) && waitForProducerThread && !task.IsCompleted)
                {
                    task.Wait();
                }
            }
        }

        private void CheckDisposed()
        {
            if (m_disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }

        private static DataResponse CreateResponseSchemeForSelect(RequestExecutionContext context)
        {
            var parsedRequest = context.ParsedRequest;
            var fields = new DataResponseField[parsedRequest.Select.OutputColumns.Count];
            var ordinal = 0;
            foreach (var clause in parsedRequest.Select.OutputColumns)
            {
                fields[ordinal] = new DataResponseField
                {
                    DataType = clause.DbType,
                    DisplayName = parsedRequest.Select.OutputColumns[ordinal].Label,
                    Name = parsedRequest.Select.OutputColumns[ordinal].Label,
                    Ordinal = ordinal
                };
                ordinal++;
            }

            return new DataResponse(fields);
        }
    }
}
