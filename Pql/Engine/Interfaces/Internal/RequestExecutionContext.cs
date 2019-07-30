using System;
using System.Linq;
using System.Threading;
using Pql.ClientDriver;
using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Wcf;
using Pql.Engine.Interfaces.Services;

namespace Pql.Engine.Interfaces.Internal
{
    public sealed class RequestExecutionContext : IDisposable
    {
        private readonly ITracer m_tracer;

        private RequestExecutionBuffer[] m_buffersRingItems;
        private DataRing<RequestExecutionBuffer> m_buffersRing;

        private CancellationTokenSource m_cancellationTokenSource;
        private IDataEngine m_engine;
        private readonly IPqlEngineHostProcess m_process;
        private PqlMessage m_requestMessage;
        private Exception m_lastError;
        private IPqlClientSecurityContext m_authContext;

        public RequestCompletion Completion { get; private set; }
        public DataRing<RequestExecutionBuffer> BuffersRing { get { return m_buffersRing; } }
        public CancellationTokenSource CancellationTokenSource { get { return m_cancellationTokenSource; } }
        public IDataEngine Engine { get { return m_engine; } }
        public DataContainerDescriptor ContainerDescriptor { get; private set; }
        public IPqlEngineHostProcess Process { get { return m_process; } }

        public IPqlClientSecurityContext AuthContext { get { return m_authContext; } }
        public PqlMessage RequestMessage { get { return m_requestMessage; } }
        public DataRequest Request { get; private set; }
        public DataRequestBulk RequestBulk { get; private set; }
        public DataRequestParams RequestParameters { get; private set; }
        public ParsedRequest ParsedRequest { get; private set; }
        public RequestExecutionContextCacheInfo CacheInfo { get; private set; }

        public IDriverDataEnumerator InputDataEnumerator { get; private set; }
        public DriverRowData DriverOutputBuffer { get; private set; }
        public ClauseEvaluationContext ClauseEvaluationContext { get; private set; }

        public RowData OutputDataBuffer { get; private set; }
        public DataResponse ResponseHeaders { get; private set; }
        
        /// <summary>
        /// Number of rows sent to client in this context.
        /// </summary>
        public int RecordsAffected;

        /// <summary>
        /// Number of rows that satisfy all criteria, before applying paging operation.
        /// </summary>
        public int TotalRowsProduced;

        public Exception LastError { get { return m_lastError; } }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="process">Parent process, receives crash notifications</param>
        /// <param name="tracer">Tracer object</param>
        public RequestExecutionContext(IPqlEngineHostProcess process, ITracer tracer)
        {
            m_tracer = tracer ?? throw new ArgumentNullException("tracer");

            m_process = process ?? throw new ArgumentNullException("process");
            
            ParsedRequest = new ParsedRequest(false);
            Request = new DataRequest();
            RequestBulk = new DataRequestBulk();
            RequestParameters = new DataRequestParams();
            
            m_buffersRingItems = new []
                {
                    new RequestExecutionBuffer(),
                    new RequestExecutionBuffer(),
                    new RequestExecutionBuffer()
                };
        }

        public void AttachResponseHeaders(DataResponse headers)
        {
            ResponseHeaders = headers ?? throw new ArgumentNullException("headers");
        }

        public void AttachContainerDescriptor(DataContainerDescriptor containerDescriptor)
        {
            ContainerDescriptor = containerDescriptor ?? throw new ArgumentNullException("containerDescriptor");
        }

        public void AttachCachedInfo(RequestExecutionContextCacheInfo cacheInfo)
        {
            CacheInfo = cacheInfo ?? throw new ArgumentNullException("cacheInfo");
        }

        public void AttachInputMessage(PqlMessage requestMessage, IDataEngine engine, IPqlClientSecurityContext authContext)
        {
            AssertIsClean();

            ClauseEvaluationContext = new ClauseEvaluationContext();

            m_authContext = authContext ?? throw new ArgumentNullException("authContext");
            m_requestMessage = requestMessage ?? throw new ArgumentNullException("requestMessage");
            m_engine = engine ?? throw new ArgumentNullException("engine");
            m_cancellationTokenSource = new CancellationTokenSource();

            // re-initialize the ring, for fault tolerance in case if previous processor fails to release items
            m_buffersRing = new DataRing<RequestExecutionBuffer>(m_buffersRingItems.Length, m_buffersRingItems.Length);
            foreach (var item in m_buffersRingItems)
            {
                // make all buffers available to producer
                m_buffersRing.AddTaskForProcessing(item, m_cancellationTokenSource.Token);
            }
        }

        public void AssertIsClean()
        {
            if (m_engine != null || m_buffersRing != null || m_cancellationTokenSource != null || m_authContext != null || m_requestMessage != null)
            {
                throw new Exception("Internal error, context has not been properly cleaned by predecessor");
            }
        }

        public void AttachDriverOutputBufferAndInputParameters(DriverRowData driverOutputBuffer, ParsedRequest parsedRequest)
        {
            if (DriverOutputBuffer != null)
            {
                throw new InvalidOperationException("Cannot reassign driver output buffer");
            }

            DriverOutputBuffer = driverOutputBuffer ?? throw new ArgumentNullException("driverOutputBuffer");
            ClauseEvaluationContext.InputRow = driverOutputBuffer;
            ClauseEvaluationContext.InputParametersRow = parsedRequest.Params.InputValues;
            ClauseEvaluationContext.InputParametersCollections = parsedRequest.Params.InputCollections;
        }

        public void AttachInputDataEnumerator(IDriverDataEnumerator inputDataEnumerator)
        {
            if (InputDataEnumerator != null)
            {
                throw new InvalidOperationException("Cannot reassign input data enumerator");
            }

            InputDataEnumerator = inputDataEnumerator ?? throw new ArgumentNullException("inputDataEnumerator");
        }

        public void PrepareBuffersForUpdate()
        {
            ClauseEvaluationContext.ChangeBuffer = new DriverChangeBuffer(
                ParsedRequest.TargetEntity.DocumentType, ParsedRequest.OrdinalOfPrimaryKey, ParsedRequest.Modify.ModifiedFields.ToArray());
            OutputDataBuffer = null;
        }

        public void PrepareChangeBufferForInsert()
        {
            PrepareBuffersForUpdate();
        }

        public void PrepareBuffersForDelete()
        {
            ClauseEvaluationContext.ChangeBuffer = new DriverChangeBuffer(ParsedRequest.TargetEntity.DocumentType);
            OutputDataBuffer = null;
        }

        public void PrepareBuffersForSelect()
        {
            ClauseEvaluationContext.ChangeBuffer = null;
            OutputDataBuffer = new RowData(ResponseHeaders.Fields.Select(x => x.DataType).ToArray());
        }

        public void Dispose()
        {
            Cleanup();

            var items = Interlocked.CompareExchange(ref m_buffersRingItems, null, m_buffersRingItems);
            if (items != null)
            {
                foreach (var item in items)
                {
                    item.Dispose();
                }
            }
        }

        public void Cleanup()
        {
            m_authContext = null;
            m_lastError = null;
            m_requestMessage = null;
            m_engine = null;
            ResponseHeaders = null;
            RecordsAffected = 0;
            TotalRowsProduced = 0;
            DriverOutputBuffer = null;
            InputDataEnumerator = null;
            ClauseEvaluationContext = null;
            OutputDataBuffer = null;
            Completion = null;
            ContainerDescriptor = null;

            CacheInfo = null;
            Request.Clear();
            RequestBulk.Clear();
            RequestParameters.Clear();
            ParsedRequest.Clear();

            var cancellation = Interlocked.CompareExchange(ref m_cancellationTokenSource, null, m_cancellationTokenSource);
            if (cancellation != null)
            {
                cancellation.Cancel();
            }

            var ring = Interlocked.CompareExchange(ref m_buffersRing, null, m_buffersRing);
            if (ring != null)
            {
                ring.Dispose();
            }

            var items = m_buffersRingItems; // do not replace with null 
            if (items != null)
            {
                foreach (var item in items)
                {
                    item.Cleanup();
                }
            }
        }

        public void Cancel(Exception lastError)
        {
            if (m_tracer.IsDebugEnabled)
            {
                m_tracer.Debug("Canceling request execution");
            }

            TrySetLastError(lastError);
            Complete(false);
        }

        public void Complete(bool waitForProducerThread)
        {
            var completion = Completion;
            if (completion != null)
            {
                Completion = null;
                completion.Discard();
            }

            Thread.MemoryBarrier();

            DriverOutputBuffer = null;
            ResponseHeaders = null;

            ParsedRequest.Clear();

            var cancellation = m_cancellationTokenSource;
            var engine = m_engine;
            var message = m_requestMessage;
            var authContext = m_authContext;

            if (cancellation != null)
            {
                cancellation.Cancel();
            }

            if (engine != null)
            {
                if (m_tracer.IsDebugEnabled)
                {
                    if (message != null && authContext != null)
                    {
                        var age = (DateTime.Now - message.CreatedOn).TotalMilliseconds;
                        m_tracer.Debug(
                            string.Format(
                                "Releasing request context for auth context id {0}, total time spent: {1} ms", authContext.ContextId, age));
                    }
                    else
                    {
                        m_tracer.Debug("Releasing request context");
                    }
                }

                engine.EndExecution(this, waitForProducerThread);
            }

            if (message != null)
            {
                message.Close();
            }

            Cleanup();
        }

        public void TrySetLastError(Exception exception)
        {
            Interlocked.CompareExchange(ref m_lastError, exception, null);
        }

        public void AttachRequestCompletion()
        {
            Completion = new RequestCompletion(this);
        }

        public class RequestCompletion : IDisposable
        {
            private RequestExecutionContext m_executionContext;

            public RequestCompletion(RequestExecutionContext executionContext)
            {
                m_executionContext = executionContext ?? throw new ArgumentNullException("executionContext");
            }

            public void Dispose()
            {
                var ctx = Interlocked.CompareExchange(ref m_executionContext, null, m_executionContext);
                if (ctx != null)
                {
                    ctx.Complete(true);
                }
            }

            public void Discard()
            {
                Interlocked.CompareExchange(ref m_executionContext, null, m_executionContext);
            }
        }

    }
}