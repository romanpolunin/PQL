using System;
using System.Configuration;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
using System.Text;
using System.Threading;
using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Wcf;
using Pql.Engine.DataContainer;
using Pql.Engine.Interfaces;
using Pql.Engine.Interfaces.Internal;
using Pql.IntegrationStubs;
using StructureMap;

namespace Pql.Server
{
    public sealed class DataServerProcess : IHostedProcess, IPqlEngineHostProcess
    {
        private ITracer m_tracer;
        private IHostingService m_host;
        private ServiceHost m_serviceHost;
        private string m_instanceName;
        private DataService m_singletonServiceInstance;
        private readonly IContainer m_container;

        public DataServerProcess(IContainer container)
        {
            m_container = container ?? throw new ArgumentNullException(nameof(container));
        }

        public void Dispose()
        {
            if (Environment.HasShutdownStarted)
            {
                return;
            }

            // let WCF start shutdown of network interface first, 
            // so that we don't get any new requests coming in
            IAsyncResult hostClosing = null;
            var host = Interlocked.CompareExchange(ref m_serviceHost, null, m_serviceHost);
            if (host != null)
            {
                try
                {
                    var state = host.State;
                    if (state == CommunicationState.Opened || state == CommunicationState.Opening)
                    {
                        hostClosing = host.BeginClose(TimeSpan.FromSeconds(1), null, null);
                    }
                }
                catch (Exception e)
                {
                    m_tracer.Exception(e);
                }
            }

            // while network shutdown is in progress, 
            // trigger cancellation of already running operations on our singleton service instance
            var service = Interlocked.CompareExchange(ref m_singletonServiceInstance, null, m_singletonServiceInstance);
            if (service != null)
            {
                service.Dispose();
            }

            // now wait for completion of network shutdown to complete
            if (hostClosing != null)
            {
                try
                {
                    host.EndClose(hostClosing);
                }
                catch (Exception e)
                {
                    m_tracer.Exception(e);

                    if (e is CommunicationException || e is TimeoutException)
                    {
                        try {host.Abort();} catch {}
                    }
                }
            }
        }

        public void Start(IHostingService host)
        {
            if (host == null)
            {
                throw new ArgumentNullException("host");
            }

            m_tracer = host.GetTracer(GetType());
            m_host = host;
            m_instanceName = GetInstanceName();

            var tcpBaseAddress = GetTcpBaseAddress();
            var httpBaseAddress = GetHealthServiceBaseAddress();

            try
            {
                PerfCounters.Remove();
                PerfCounters.Install();
            }
            catch (Exception e)
            {
                m_tracer.Exception(e);
                throw;
            }

            var maxConcurrency = Environment.ProcessorCount * 16;
            var maxPending = Environment.ProcessorCount * 16;

            m_singletonServiceInstance = new DataService(
                m_container, m_host.GetTracer(typeof(DataService)), this, tcpBaseAddress.Authority + ", " + m_instanceName, maxConcurrency, null);

            var serviceHost = CreateServiceHost(m_singletonServiceInstance, new[] { tcpBaseAddress, httpBaseAddress }, maxConcurrency, maxPending);
            serviceHost.Open();
            m_serviceHost = serviceHost;

            if (m_tracer.IsInfoEnabled)
            {
                var builder = new StringBuilder(200);
                foreach (var ep in serviceHost.ChannelDispatchers)
                {
                    if (ep.Listener != null)
                    {
                        builder.AppendLine(ep.Listener.Uri.ToString());
                    }
                }
                m_tracer.InfoFormat("Service host [{0}] started. Listening on following base URIs:{1}{2}",
                    m_instanceName, Environment.NewLine, builder);
            }
        }

        public void Pause()
        {
            throw new NotImplementedException();
        }

        public void Continue()
        {
            throw new NotImplementedException();
        }

        private string GetInstanceName()
        {
            var name = ConfigurationManager.AppSettings["PqlProcessorInstanceName"];
            return string.IsNullOrEmpty(name) ? "default" : name;
        }

        private Uri GetHealthServiceBaseAddress()
        {
            var address = ConfigurationManager.AppSettings["healthServiceBaseAddress"];
            if (string.IsNullOrEmpty(address))
            {
                address = "http://localhost:82";
            }

            return new Uri(address, UriKind.Absolute);
        }

        private Uri GetTcpBaseAddress()
        {
            var address = ConfigurationManager.AppSettings["PqlProcessorBaseAddress"];
            if (string.IsNullOrEmpty(address))
            {
                address = "net.tcp://localhost:5000";
            }

            return new Uri(address, UriKind.Absolute);
        }

        private static ServiceHost CreateServiceHost(DataService singletonServiceInstance, Uri[] baseAddresses, int maxConcurrency, int maxPending)
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

            serviceHost.AddServiceEndpoint(typeof(IDataService), GetBinding(maxPending), String.Empty);
            return serviceHost;
        }

        public void HandleFailNode(string reason)
        {
            m_host.HandleHostedProcessFailure(this, reason);
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
                    ListenBacklog = maxPending/2,
                    MaxPendingAccepts = maxPending/2,
                    MaxPendingConnections = maxPending,
                    PortSharingEnabled = true
                };

            var binding = new CustomBinding(new PqlMessageEncodingBindingElement(), transport)
                {
                    OpenTimeout = TimeSpan.FromMinutes(1),
                    CloseTimeout = TimeSpan.FromMinutes(1),
                    ReceiveTimeout = TimeSpan.FromMinutes(60),
                    SendTimeout = TimeSpan.FromMinutes(60)
                };

            return binding;
        }

        // This attribute can be used to install a custom error handler for a service.
    }
}
