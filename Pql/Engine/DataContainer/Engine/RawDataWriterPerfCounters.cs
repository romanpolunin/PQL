using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Pql.Engine.Interfaces.Internal;

namespace Pql.Engine.DataContainer.Engine
{
    internal sealed class RawDataWriterPerfCounters : IDisposable
    {
        private List<PerformanceCounter> m_counters;

        public readonly PerformanceCounter TotalBytes;
        public readonly PerformanceCounter ByteRate;
        public readonly PerformanceCounter TotalRows;
        public readonly PerformanceCounter RowRate;

        public RawDataWriterPerfCounters(string instanceName)
        {
            if (string.IsNullOrEmpty(instanceName))
            {
                throw new ArgumentNullException("instanceName");
            }

            m_counters = new List<PerformanceCounter>(4);

            TotalBytes = CreatePerformanceCounter(instanceName, PerfCounters.CounterTotalBytesOutputName);
            ByteRate = CreatePerformanceCounter(instanceName, PerfCounters.CounterBytesOutputRateName);
            TotalRows = CreatePerformanceCounter(instanceName, PerfCounters.CounterTotalRowsOutputName);
            RowRate = CreatePerformanceCounter(instanceName, PerfCounters.CounterRowsOutputRateName);
        }

        private PerformanceCounter CreatePerformanceCounter(string instanceName, string counterName)
        {
            var counter = new PerformanceCounter
                {
                    InstanceLifetime = PerformanceCounterInstanceLifetime.Process,
                    CategoryName = PerfCounters.CategoryPqlDataServerName,
                    CounterName = counterName,
                    InstanceName = instanceName,
                    ReadOnly = false
                };
            m_counters.Add(counter);
            return counter;
        }

        public void Dispose()
        {
            var counters = Interlocked.CompareExchange(ref m_counters, null, m_counters);
            if (counters != null)
            {
                try
                {
                    foreach (var counter in counters)
                    {
                        //counter.RemoveInstance();
                        counter.Dispose();
                    }
                }
                finally
                {
                    counters.Clear();
                }
            }
        }
    }
}