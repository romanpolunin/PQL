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
        private readonly ITracer _tracer;

        private RequestExecutionBuffer[] _buffersRingItems;
        private DataRing<RequestExecutionBuffer> _buffersRing;

        private CancellationTokenSource _cancellationTokenSource;
        private IDataEngine _engine;
        private readonly IPqlEngineHostProcess _process;
        private PqlMessage _requestMessage;
        private Exception _lastError;
        private IPqlClientSecurityContext _authContext;

        public RequestCompletion Completion { get; private set; }
        public DataRing<RequestExecutionBuffer> BuffersRing => _buffersRing;
        public CancellationTokenSource CancellationTokenSource => _cancellationTokenSource;
        public IDataEngine Engine => _engine;
        public DataContainerDescriptor ContainerDescriptor { get; private set; }
        public IPqlEngineHostProcess Process => _process;

        public IPqlClientSecurityContext AuthContext => _authContext;
        public PqlMessage RequestMessage => _requestMessage;
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

        public Exception LastError => _lastError;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="process">Parent process, receives crash notifications</param>
        /// <param name="tracer">Tracer object</param>
        public RequestExecutionContext(IPqlEngineHostProcess process, ITracer tracer)
        {
            _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));

            _process = process ?? throw new ArgumentNullException(nameof(process));
            
            ParsedRequest = new ParsedRequest(false);
            Request = new DataRequest();
            RequestBulk = new DataRequestBulk();
            RequestParameters = new DataRequestParams();
            
            _buffersRingItems = new []
                {
                    new RequestExecutionBuffer(),
                    new RequestExecutionBuffer(),
                    new RequestExecutionBuffer()
                };
        }

        public void AttachResponseHeaders(DataResponse headers)
        {
            ResponseHeaders = headers ?? throw new ArgumentNullException(nameof(headers));
        }

        public void AttachContainerDescriptor(DataContainerDescriptor containerDescriptor)
        {
            ContainerDescriptor = containerDescriptor ?? throw new ArgumentNullException(nameof(containerDescriptor));
        }

        public void AttachCachedInfo(RequestExecutionContextCacheInfo cacheInfo)
        {
            CacheInfo = cacheInfo ?? throw new ArgumentNullException(nameof(cacheInfo));
        }

        public void AttachInputMessage(PqlMessage requestMessage, IDataEngine engine, IPqlClientSecurityContext authContext)
        {
            AssertIsClean();

            ClauseEvaluationContext = new ClauseEvaluationContext();

            _authContext = authContext ?? throw new ArgumentNullException(nameof(authContext));
            _requestMessage = requestMessage ?? throw new ArgumentNullException(nameof(requestMessage));
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _cancellationTokenSource = new CancellationTokenSource();

            // re-initialize the ring, for fault tolerance in case if previous processor fails to release items
            _buffersRing = new DataRing<RequestExecutionBuffer>(_buffersRingItems.Length, _buffersRingItems.Length);
            foreach (var item in _buffersRingItems)
            {
                // make all buffers available to producer
                _buffersRing.AddTaskForProcessing(item, _cancellationTokenSource.Token);
            }
        }

        public void AssertIsClean()
        {
            if (_engine != null || _buffersRing != null || _cancellationTokenSource != null || _authContext != null || _requestMessage != null)
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

            DriverOutputBuffer = driverOutputBuffer ?? throw new ArgumentNullException(nameof(driverOutputBuffer));
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

            InputDataEnumerator = inputDataEnumerator ?? throw new ArgumentNullException(nameof(inputDataEnumerator));
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

            var items = Interlocked.CompareExchange(ref _buffersRingItems, null, _buffersRingItems);
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
            _authContext = null;
            _lastError = null;
            _requestMessage = null;
            _engine = null;
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

            var cancellation = Interlocked.CompareExchange(ref _cancellationTokenSource, null, _cancellationTokenSource);
            if (cancellation != null)
            {
                cancellation.Cancel();
            }

            var ring = Interlocked.CompareExchange(ref _buffersRing, null, _buffersRing);
            if (ring != null)
            {
                ring.Dispose();
            }

            var items = _buffersRingItems; // do not replace with null 
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
            if (_tracer.IsDebugEnabled)
            {
                _tracer.Debug("Canceling request execution");
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

            var cancellation = _cancellationTokenSource;
            var engine = _engine;
            var message = _requestMessage;
            var authContext = _authContext;

            if (cancellation != null)
            {
                cancellation.Cancel();
            }

            if (engine != null)
            {
                if (_tracer.IsDebugEnabled)
                {
                    if (message != null && authContext != null)
                    {
                        var age = (DateTime.Now - message.CreatedOn).TotalMilliseconds;
                        _tracer.Debug(
                            string.Format(
                                "Releasing request context for auth context id {0}, total time spent: {1} ms", authContext.ContextId, age));
                    }
                    else
                    {
                        _tracer.Debug("Releasing request context");
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
            Interlocked.CompareExchange(ref _lastError, exception, null);
        }

        public void AttachRequestCompletion()
        {
            Completion = new RequestCompletion(this);
        }

        public class RequestCompletion : IDisposable
        {
            private RequestExecutionContext _executionContext;

            public RequestCompletion(RequestExecutionContext executionContext)
            {
                _executionContext = executionContext ?? throw new ArgumentNullException(nameof(executionContext));
            }

            public void Dispose()
            {
                var ctx = Interlocked.CompareExchange(ref _executionContext, null, _executionContext);
                if (ctx != null)
                {
                    ctx.Complete(true);
                }
            }

            public void Discard()
            {
                Interlocked.CompareExchange(ref _executionContext, null, _executionContext);
            }
        }

    }
}