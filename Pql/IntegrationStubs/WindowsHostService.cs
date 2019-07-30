using System;
using System.Linq;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;
using Pql.Engine.Interfaces;
using StructureMap;

namespace Pql.IntegrationStubs
{
    internal partial class WindowsHostService : ServiceBase, IHostingService
    {
        private readonly ITracer m_tracer;
        private readonly IContainer m_container;
        private IHostedProcess[] m_hostedInstances;
        private long m_haveToStop;

        public WindowsHostService(IContainer container)
        {
            InitializeComponent();

            m_tracer = new DummyTracer();
            m_container = container ?? throw new ArgumentNullException(nameof(container));
        }

        protected override void OnStart(string[] args)
        {
            if (m_hostedInstances != null)
            {
                throw new InvalidOperationException("Cannot start more than once");
            }

            m_tracer.Info("Resolving process implementations...");

            m_hostedInstances = m_container.GetAllInstances<IHostedProcess>().ToArray();
            if (m_hostedInstances == null || m_hostedInstances.Length == 0)
            {
                throw new Exception("Could not resolve any implementations of " + typeof(IHostedProcess).FullName);
            }

            m_tracer.Info("Starting process implementations...");

            try
            {
                foreach (var process in m_hostedInstances)
                {
                    m_tracer.Info("Starting: " + process.GetType().AssemblyQualifiedName);
                    process.Start(this);
                }
            }
            catch (Exception e)
            {
                m_tracer.Exception("Failed to initialize process implementations, will clean up and throw.", e);
                HandleProcessFailure("Failed during startup");
            }

            m_tracer.Info("Started");
        }

        protected override void OnPause()
        {
            m_tracer.Info("Pausing process implementations...");

            try
            {
                foreach (var process in m_hostedInstances)
                {
                    m_tracer.Info("Pause: " + process.GetType().AssemblyQualifiedName);
                    process.Pause();
                }
            }
            catch (Exception e)
            {
                m_tracer.Exception("Failed to pause process implementations, will clean up and throw.", e);
                HandleProcessFailure("Failed during pause");
            }

            m_tracer.Info("Paused");
        }

        protected override void OnContinue()
        {
            m_tracer.Info("Continuing process implementations...");

            try
            {
                foreach (var process in m_hostedInstances)
                {
                    m_tracer.Info("Continue: " + process.GetType().AssemblyQualifiedName);
                    process.Continue();
                }
            }
            catch (Exception e)
            {
                m_tracer.Exception("Failed to continue process implementations, will clean up and throw.", e);
                HandleProcessFailure("Failed during continue");
            }

            m_tracer.Info("Continued");
        }

        protected override void OnStop()
        {
            var hostedInstances = Interlocked.CompareExchange(ref m_hostedInstances, null, m_hostedInstances);
            if (hostedInstances == null)
            {
                return;
            }

            m_tracer.Info("Stopping process implementations...");

            foreach (var process in hostedInstances)
            {
                m_tracer.Info("Stopping: " + process.GetType().AssemblyQualifiedName);
                process.Dispose();
            }

            m_tracer.Info("All process implementations stopped.");
        }

        public void RunInteractive(string[] args)
        {
            m_tracer.Info("Starting in console mode...");

            OnStart(args);

            m_tracer.Info("Running in console mode. Press any key to stop.");
            while (!Console.KeyAvailable && 0 == Interlocked.Read(ref m_haveToStop))
            {
                Thread.Sleep(500);
            }

            OnStop();

            m_tracer.Info("Waiting 10s on process to stop...");
            Thread.Sleep(10000);
        }

        #region Implementation of IHostingService

        public void HandleHostedProcessFailure(IHostedProcess sender, string reason)
        {
            HandleProcessFailure(reason);
        }

        public ITracer GetTracer(Type getType)
        {
            return new DummyTracer();
        }

        private void HandleProcessFailure(string reason)
        {
            // prevent reentrancy
            if (0 != Interlocked.CompareExchange(ref m_haveToStop, 1, 0))
            {
                m_tracer.Exception("Received another stop error during shutdown: " + reason, null);
                return;
            }

            if (Environment.HasShutdownStarted)
            {
                m_tracer.Exception("Received a stop error during shutdown, but will not do anything: " + reason, null);
            }
            else
            {
                m_tracer.Exception("Have to stop because of error, " + reason, null);
                new Task(Stop).Start();
            }
        }

        #endregion
    }
}
