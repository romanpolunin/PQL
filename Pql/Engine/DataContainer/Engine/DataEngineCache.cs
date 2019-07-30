using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Pql.Engine.Interfaces;
using Pql.Engine.Interfaces.Internal;
using Pql.Engine.Interfaces.Services;
using StructureMap;

namespace Pql.Engine.DataContainer.Engine
{
    public sealed class DataEngineCache : IDataEngineCache
    {
        private readonly IContainer m_container;
        private readonly ITracer m_tracer;
        private volatile ConcurrentDictionary<string, ConcurrentDictionary<string, IDataEngine>> m_engines;
        private readonly string m_instanceName;
        private readonly int m_maxEngineConcurrency;
        private readonly string m_storageDriverKey;
        private Timer m_timer;

        public DataEngineCache(IContainer container, ITracer tracer, string instanceName, int maxEngineConcurrency)
        {
            if (string.IsNullOrEmpty(instanceName))
            {
                throw new ArgumentNullException("instanceName");
            }

            if (maxEngineConcurrency <= 0 || maxEngineConcurrency > 10000)
            {
                throw new ArgumentOutOfRangeException("maxEngineConcurrency");
            }

            m_container = container ?? throw new ArgumentNullException(nameof(container));
            m_tracer = tracer ?? throw new ArgumentNullException("tracer");
            m_instanceName = instanceName;
            m_maxEngineConcurrency = maxEngineConcurrency;

            m_storageDriverKey = ConfigurationManager.AppSettings["storageDriverKey"];
            
            if (string.IsNullOrEmpty(m_storageDriverKey))
            {
                throw new InvalidOperationException("appSetting value for key 'storageDriverKey' is not set");
            }

            if (null == m_container.TryGetInstance<IStorageDriverFactory>(m_storageDriverKey))
            {
                throw new ArgumentException("Container does not have a factory for storage driver " + m_storageDriverKey);
            }

            m_engines = new ConcurrentDictionary<string, ConcurrentDictionary<string, IDataEngine>>();
            m_timer = new Timer(OnTimerCallback, this, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));
        }

        public int GetTotalEnginesCount()
        {
            return m_engines.Sum(x => x.Value.Values.Count);
        }

        public void Dispose()
        {
            var timer = Interlocked.CompareExchange(ref m_timer, null, m_timer);
            if (timer != null)
            {
                timer.Dispose();
            }

            var temp = Interlocked.CompareExchange(ref m_engines, null, m_engines);
            if (temp == null)
            {
                return;
            }

            foreach (var tenantEngines in temp.Values)
            {
                foreach (var engine in tenantEngines.Values)
                {
                    engine.Dispose();
                }

                tenantEngines.Clear();
            }

            temp.Clear();
        }

        public IDataEngine GetEngine(string tenantId, string scopeId)
        {
            if (string.IsNullOrEmpty(tenantId))
            {
                throw new ArgumentNullException("tenantId");
            }

            if (string.IsNullOrEmpty(scopeId))
            {
                throw new ArgumentNullException("scopeId");
            }

            // avoid using GetOrAdd because it may invoke factory method multiple times
            if (!m_engines.TryGetValue(tenantId, out var tenantEngines))
            {
                lock (m_engines)
                {
                    if (!m_engines.TryGetValue(tenantId, out tenantEngines))
                    {
                        tenantEngines = new ConcurrentDictionary<string, IDataEngine>();
                        if (!m_engines.TryAdd(tenantId, tenantEngines))
                        {
                            throw new Exception("Failed to add tenant-specific cache of engines for tenant " + tenantId);
                        }
                    }

                    if (tenantEngines == null)
                    {
                        throw new Exception("tenantEngines is null, tenant " + tenantId);
                    }
                }
            }

            // avoid using GetOrAdd because it may invoke factory method multiple times
            if (!tenantEngines.TryGetValue(scopeId, out var engine))
            {
                lock (tenantEngines)
                {
                    if (!tenantEngines.TryGetValue(scopeId, out engine))
                    {
                        var engineCount = GetTotalEnginesCount();
                        if (engineCount > MaxTotalEnginesCount)
                        {
                            new Task(x => ForceExpireOneEngine((DataEngineCache)x), this).Start();
                        }

                        m_tracer.InfoFormat("Creating new engine for tenant {0}, scope {1}", tenantId, scopeId);
                        var factory = m_container.GetInstance<IStorageDriverFactory>(m_storageDriverKey);
                        var storageDriver = factory.Create();
                        storageDriver.Initialize(m_tracer, GetDriverConnectionConfig(scopeId, factory));

                        engine = new DataEngine(m_tracer, m_instanceName, m_maxEngineConcurrency, storageDriver, RequireDescriptor(storageDriver));

                        if (!tenantEngines.TryAdd(scopeId, engine))
                        {
                            throw new Exception(string.Format("Failed to add new engine for tenant {0}, scope {1}", tenantId, scopeId));
                        }
                    }

                    if (engine == null)
                    {
                        throw new Exception(string.Format("Engine is null for tenant {0}, scope {1}", tenantId, scopeId));
                    }
                }
            }

            return engine;
        }

        private static DataContainerDescriptor RequireDescriptor(IStorageDriver driver)
        {
            var descriptor = driver.GetDescriptor();
            if (descriptor == null)
            {
                throw new InvalidOperationException("Driver reports that container descriptor is not initialized");
            }
            return descriptor;
        }


        private void OnTimerCallback(object state)
        {
            var cache = (DataEngineCache)state;
            var engines = cache.m_engines;
            if (engines == null)
            {
                return;
            }

            var shouldLogActiveContexts = m_tracer.IsInfoEnabled;

            // unload engines that were not used for more than 10 minutes
            var now = DateTime.UtcNow;
            foreach (var tenantEnginesRec in engines.ToArray())
            {
                foreach (var engineRec in tenantEnginesRec.Value.ToArray())
                {
                    if (shouldLogActiveContexts)
                    {
                        m_tracer.InfoFormat("------ Begin state information for engine on tenant {0}, scope {1}", tenantEnginesRec.Key, engineRec.Key);
                        engineRec.Value.WriteStateInfoToLog();
                        m_tracer.InfoFormat("------ End state information for engine on tenant {0}, scope {1}", tenantEnginesRec.Key, engineRec.Key);
                    }
                    
                    var diff = now - engineRec.Value.UtcLastUsedAt;
                    if (diff.TotalMinutes > MaxEngineUnusedAgeMinutes)
                    {
                        cache.m_tracer.Info(string.Format("Aging out engine for tenant {0}, scope {1}", tenantEnginesRec.Key, engineRec.Key));

                        if (tenantEnginesRec.Value.TryRemove(engineRec.Key, out var engine))
                        {
                            engine.Dispose();
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Will find the eldest engine and force-purge it.
        /// </summary>
        private static void ForceExpireOneEngine(DataEngineCache cache)
        {
            var engines = cache.m_engines;
            if (engines == null)
            {
                return;
            }

            IDataEngine oldest = null;
            ConcurrentDictionary<string, IDataEngine> container = null;
            string scope = null, tenant = null;

            foreach (var tenantEnginesRec in engines)
            {
                foreach (var engineRec in tenantEnginesRec.Value)
                {
                    if (oldest == null || oldest.UtcLastUsedAt > engineRec.Value.UtcLastUsedAt)
                    {
                        oldest = engineRec.Value;
                        container = tenantEnginesRec.Value;
                        tenant = tenantEnginesRec.Key;
                        scope = engineRec.Key;
                    }
                }
            }

            if (oldest != null)
            {
                cache.m_tracer.InfoFormat("Forced expiration of engine for tenant {0}, scope {1}", tenant, scope);

                if (container.TryRemove(scope, out var engine))
                {
                    // only invoke Dispose here if the engine is not active right now
                    // otherwise, let it be garbage-collected
                    var ageseconds = (DateTime.UtcNow - engine.UtcLastUsedAt).TotalSeconds;
                    if (ageseconds < 0)
                    {
                        ageseconds = -ageseconds;
                    }

                    if (ageseconds > 1.0)
                    {
                        engine.Dispose();
                    }
                }
            }
        }

        private int MaxTotalEnginesCount
        {
            get { return 1000; }
        }

        private static int MaxEngineUnusedAgeMinutes
        {
            get { return 10; }
        }

        private object GetDriverConnectionConfig(string scopeId, IStorageDriverFactory storageDriverFactory)
        {
            // is it overridden in config?
            var value = ConfigurationManager.AppSettings["storageDriverInitString"];
            return string.IsNullOrEmpty(value) ? storageDriverFactory.GetDriverConfig(scopeId) : value;
        }
    }
}