using Pql.ExpressionEngine.Utilities;
using Pql.SqlEngine.DataContainer.Engine;
using Pql.SqlEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Services;

namespace Pql.Engine.DataContainer
{
    public sealed class DataService : IDataService, IDisposable
    {
        private readonly IPqlEngineHostProcess _process;
        private readonly string _instanceName;
        private readonly IContainer _container;
        private readonly ITracer _tracer;
        private readonly IDataEngineCache _enginesCache;
        private readonly int _maxEngineConcurrency;
        private readonly RequestProcessingManager[] _requestManagers;
        private readonly ObjectPool<RequestProcessingManager> _requestManagersPool;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly string _protocolVersion;

        public DataService(IContainer container, ITracer tracer, IPqlEngineHostProcess process, string instanceName, int maxEngineConcurrency, IDataEngineCache dataEngineCache)
        {
            if (string.IsNullOrEmpty(instanceName))
            {
                throw new ArgumentNullException(nameof(instanceName));
            }

            if (maxEngineConcurrency is <= 0 or > 10000)
            {
                throw new ArgumentOutOfRangeException(nameof(maxEngineConcurrency), maxEngineConcurrency, "Invalid value");
            }

            _protocolVersion = "default";
            _container = container ?? throw new ArgumentNullException(nameof(container));
            _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));

            _cancellationTokenSource = new CancellationTokenSource();
            _process = process ?? throw new ArgumentNullException(nameof(process));
            _instanceName = instanceName;
            _maxEngineConcurrency = maxEngineConcurrency;

            _requestManagers = new RequestProcessingManager[maxEngineConcurrency];

            // request processing managers will not be dynamically created, 
            // this is to explicitly limit concurrency regardless of service infrastructure settings
            _requestManagersPool = new ObjectPool<RequestProcessingManager>(_maxEngineConcurrency, null);
            for (var i = 0; i < _requestManagersPool.Capacity; i++)
            {
                _requestManagers[i] = new RequestProcessingManager(_tracer, _process);
                _requestManagersPool.Return(_requestManagers[i]);
            }

            _enginesCache = dataEngineCache ?? new DataEngineCache(_container, _tracer, _instanceName, _maxEngineConcurrency);
        }

        public Message Process(Message message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            if (!(message is PqlMessage pqlMessage))
            {
                throw new Exception(string.Format("Invalid message type, expected {0}, got {1}",
                    typeof(PqlMessage).AssemblyQualifiedName, message.GetType().AssemblyQualifiedName));
            }

            try
            {
                // re-establish authentication context as it is on the client side
                var authContext = AuthContextSerializer.GetObject(pqlMessage.AuthTicket);
                PqlEngineSecurityContext.Set(authContext);

                // get a processing manager from pool, start production
                var holder = _requestManagersPool.Take(_cancellationTokenSource.Token);
                try
                {
                    var engine = _enginesCache.GetEngine(authContext.TenantId, pqlMessage.ScopeId);
                    holder.Item.Attach((PqlMessage) message, engine, authContext);
                    engine.BeginExecution(holder.Item.ExecutionContext);
                }
                catch
                {
                    holder.Item.ExecutionContext.Cancel(null);
                    holder.Dispose();
                    throw;
                }

                // return the message to WCF infrastructure
                try
                {
                    holder.Item.ExecutionContext.AttachRequestCompletion();
                    return new PqlMessage(
                        holder.Item, 
                        new IDisposable[]
                            {
                                holder.Item.ExecutionContext.Completion, 
                                holder
                            }, 
                        pqlMessage.AuthTicket, pqlMessage.ScopeId, _protocolVersion);
                }
                catch (Exception e)
                {
                    holder.Item.ExecutionContext.Cancel(e);
                    holder.Dispose();
                    throw;
                }
            }
            catch (Exception e)
            {
                _tracer.Exception(e);
                return new PqlMessage(new PqlErrorDataWriter(1, e, true), null, pqlMessage.AuthTicket, pqlMessage.ScopeId, _protocolVersion);
            }
        }

        public void Dispose()
        {
            // first: signal cancellation to incoming callers on this class - those which have not yet started processing
            if (_cancellationTokenSource != null)
            {
                _cancellationTokenSource.Cancel();
            }

            // second: dispose engine - this will signal cancellation to all actively executing requests
            if (_enginesCache != null)
            {
                _enginesCache.Dispose();
            }

            // dispose the pool of contexts
            if (_requestManagers != null)
            {
                foreach (var item in _requestManagers)
                {
                    item.Dispose();
                }
            }
        }
    }
}