using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Wcf;
using Pql.Engine.DataContainer;
using Pql.Engine.DataContainer.Engine;
using Pql.Engine.DataContainer.RamDriver;
using Pql.IntegrationStubs;

namespace Pql.Engine.UnitTest
{
    public class TestServiceContainer : IDisposable
    {
        public const string TestHostBaseAddressTcp = "localhost:5000";
        public const string TestHostBaseAddressHttp = "localhost:5001";
        public const string TestHostInstanceName = "default"; //"MyUnitTestInstance";

        public ServiceHost DataServiceHost { get; set; }

        public string ServiceInstanceName { get; set; }

        public DataService DataService { get; set; }

        public TestableEngineCache EngineCache { get; set; }

        public DataEngine Engine { get; set; }

        public RamDriver StorageDriver { get; set; }

        public TestServiceContainer()
        {
            StorageDriver = new RamDriver();
            StorageDriver.Initialize(
                new DummyTracer(),
                new RamDriverSettings
                {
                    Descriptor = DataGen.BuildContainerDescriptor(),
                    StorageRoot = ConfigurationManager.AppSettings["storageDriverInitString"]
                }
                );

            const int maxconcurrency = 16;

            ServiceInstanceName = TestHostInstanceName;
            Engine = new DataEngine(
                new DummyTracer(), 
                ServiceInstanceName + "-Engine", maxconcurrency, StorageDriver, StorageDriver.GetDescriptor());
            EngineCache = new TestableEngineCache(Engine);
            DataService = new DataService(
                new DummyTracer(), 
                new DummyHostedProcess(), ServiceInstanceName, maxconcurrency, EngineCache);
            DataServiceHost = CreateServiceHost(DataService, new[]
                {
                    new Uri("net.tcp://" + TestHostBaseAddressTcp)
                }, maxconcurrency, ServiceInstanceName, maxconcurrency);
            DataServiceHost.Opened += DataServiceHostOnOpened;
            DataServiceHost.Open();
        }

        private void DataServiceHostOnOpened(object sender, EventArgs eventArgs)
        {
            foreach (var disp in DataServiceHost.ChannelDispatchers)
            {
                Console.WriteLine("Listening on: " + disp.Listener.Uri);
            }
        }

        private static ServiceHost CreateServiceHost(DataService singletonServiceInstance, Uri[] baseAddresses, int maxConcurrency, string instanceName, int maxPending)
        {
            if (maxConcurrency < 1 || maxConcurrency > 10000)
            {
                throw new ArgumentOutOfRangeException("maxConcurrency", maxConcurrency, "Invalid value");
            }

            var serviceHost = new WebServiceHost(singletonServiceInstance, baseAddresses);
            var serviceThrottlingBehavior = new ServiceThrottlingBehavior
                {
                    MaxConcurrentInstances = maxConcurrency,
                    MaxConcurrentCalls = maxConcurrency,
                    MaxConcurrentSessions = maxConcurrency
                };
            serviceHost.Description.Behaviors.Add(serviceThrottlingBehavior);

            serviceHost.AddServiceEndpoint(typeof(IDataService), GetBinding(maxPending), instanceName);
            return serviceHost;
        }

        private static Binding GetBinding(int maxPending)
        {
            if (maxPending < Environment.ProcessorCount || maxPending > 1000)
            {
                throw new ArgumentOutOfRangeException("maxPending", maxPending, "Invalid value");
            }

            var transport = new TcpTransportBindingElement
                {
                    HostNameComparisonMode = HostNameComparisonMode.WeakWildcard,
                    TransferMode = TransferMode.Streamed,
                    ManualAddressing = true,
                    MaxReceivedMessageSize = long.MaxValue,
                    ListenBacklog = maxPending / 2,
                    MaxPendingAccepts = maxPending / 2,
                    MaxPendingConnections = maxPending,
                    PortSharingEnabled = true
                };

            var binding = new CustomBinding(new PqlMessageEncodingBindingElement(), transport)
                {
                    OpenTimeout = TimeSpan.FromMinutes(1),
                    CloseTimeout = TimeSpan.FromMinutes(1),
                    ReceiveTimeout = TimeSpan.MaxValue,
                    SendTimeout = TimeSpan.MaxValue
                };

            return binding;
        }

        public void Dispose()
        {
            if (DataService != null)
            {
                DataService.Dispose();
                DataService = null;
            }

            if (StorageDriver != null)
            {
                StorageDriver.Dispose();
                StorageDriver = null;
            }
        }
    }
}