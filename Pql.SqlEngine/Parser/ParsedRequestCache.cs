using System;
using System.Collections.Specialized;
using System.Globalization;
using System.Runtime.Caching;
using System.Runtime.CompilerServices;
using Pql.ClientDriver.Protocol;
using Pql.Engine.Interfaces.Internal;

namespace Pql.Engine.DataContainer.Parser
{
    internal sealed class ParsedRequestCache : IDisposable
    {
        private readonly MemoryCache m_generalRequestCache;
        private readonly MemoryCache m_parameterizedRequestCache;
        private readonly CacheItemPolicy m_defaultPolicy;

        public ParsedRequestCache(string instanceName)
        {
            if (string.IsNullOrEmpty(instanceName))
            {
                throw new ArgumentNullException(nameof(instanceName));
            }

            var config = new NameValueCollection(1) { { "CacheMemoryLimitMegabytes", "200" } };
            m_defaultPolicy = new CacheItemPolicy { SlidingExpiration = TimeSpan.FromMinutes(10) };
            m_generalRequestCache = new MemoryCache(instanceName + "-GeneralRequestCache", config);
            m_parameterizedRequestCache = new MemoryCache(instanceName + "-ParameterizedRequestCache", config);
        }

        /// <summary>
        /// Computes an Int64 hash value for a request for use with dictionary of prepared requests.
        /// </summary>
        public static long GetRequestHash(DataRequest request, DataRequestBulk requestBulk, DataRequestParams requestParams)
        {
            if (request == null)
            {
                throw new ArgumentNullException(nameof(request));
            }

            if (request.HaveRequestBulk && requestBulk == null)
            {
                throw new ArgumentNullException(nameof(requestBulk));
            }

            if (request.HaveParameters && requestParams == null)
            {
                throw new ArgumentNullException(nameof(requestParams));
            }

            InitHash(out var hash, out var modulo);

            if (request.HaveRequestBulk)
            {
                AppendToHash(ref hash, modulo, (requestBulk.EntityName ?? string.Empty).GetHashCode());
                AppendToHash(ref hash, modulo, (int)requestBulk.DbStatementType);

                foreach (var f in requestBulk.FieldNames)
                {
                    AppendToHash(ref hash, modulo, f.GetHashCode());
                }
            }
            else
            {
                AppendToHash(ref hash, modulo, (request.CommandText ?? string.Empty).GetHashCode());

                if (request.HaveParameters)
                {
                    foreach (var dataType in requestParams.DataTypes)
                    {
                        AppendToHash(ref hash, modulo, (int)dataType);
                    }

                    foreach (var bitVectorData in requestParams.IsCollectionFlags)
                    {
                        AppendToHash(ref hash, modulo, bitVectorData);
                    }
                }
            }

            return hash;
        }

        
        public static void InitHash(out long initial, out int modulo)
        {
            modulo = 16777619;
            initial = 2166136261;
        }

        
        public static void AppendToHash(ref long current, int modulo, int add)
        {
            unchecked
            {
                current = (current ^ add) * modulo;
            }
        }

        public RequestExecutionContextCacheInfo AddOrGetExisting(long hashCode, bool parameterized)
        {
            var cache = parameterized ? m_parameterizedRequestCache : m_generalRequestCache;

            var key = hashCode.ToString(CultureInfo.InvariantCulture);
            
            // it is quite likely that we already have this cacheInfo instance
            var obj = (RequestExecutionContextCacheInfo)cache.Get(key);
            
            if (obj == null)
            {
                obj = new RequestExecutionContextCacheInfo(hashCode);
                var prev = (RequestExecutionContextCacheInfo)cache.AddOrGetExisting(key, obj, m_defaultPolicy);

                if (prev != null)
                {
                    obj = prev;
                }
            }

            return obj;
        }

        public void Dispose()
        {
        }
    }
}
