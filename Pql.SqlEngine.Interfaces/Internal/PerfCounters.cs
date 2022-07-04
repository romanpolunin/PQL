using System.Diagnostics;

namespace Pql.Engine.Interfaces.Internal
{
    public static class PerfCounters
    {
        public static void Remove()
        {
            if (PerformanceCounterCategory.Exists(CategoryPqlDataServerName))
            {
                PerformanceCounterCategory.Delete(CategoryPqlDataServerName);
            }
        }

        public static void Install()
        {
            if (!PerformanceCounterCategory.Exists(CategoryPqlDataServerName))
            {
                var counterDatas = new CounterCreationDataCollection
                    {
                        new CounterCreationData(CounterTotalRowsOutputName, CounterTotalRowsOutputName, PerformanceCounterType.NumberOfItems64),
                        new CounterCreationData(CounterRowsOutputRateName, CounterRowsOutputRateName, PerformanceCounterType.RateOfCountsPerSecond32),
                        new CounterCreationData(CounterTotalBytesOutputName, CounterTotalBytesOutputName, PerformanceCounterType.NumberOfItems64),
                        new CounterCreationData(CounterBytesOutputRateName, CounterBytesOutputRateName, PerformanceCounterType.RateOfCountsPerSecond64),
                    };

                PerformanceCounterCategory.Create(
                    CategoryPqlDataServerName, CategoryPqlDataServerHelp, PerformanceCounterCategoryType.MultiInstance, counterDatas);
            }
        }

        public const string CategoryPqlDataServerName = "Pql Data Server";
        public const string CategoryPqlDataServerHelp = "Pql Data Server Statistics";
        public const string CounterTotalRowsOutputName = "Total rows sent to clients";
        public const string CounterTotalBytesOutputName = "Total bytes sent to client";
        public const string CounterBytesOutputRateName = "Combined bytes per second";
        public const string CounterRowsOutputRateName = "Combined rows per second";
    }
}
