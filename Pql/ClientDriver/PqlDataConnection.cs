using System;
using System.Data;
using System.Data.Common;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.Threading;
using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Wcf;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Implements a connection to PQL server.
    /// </summary>
    public sealed class PqlDataConnection : DbConnection
    {
        private ChannelFactory<IDataService> m_channelFactory;
        private ConnectionProps m_connectionProps;
        private IDataService m_channel;
        private ConnectionState m_connectionState;
        private PqlDataCommand m_activeCommand;
        private CancellationTokenSource m_cancellationTokenSource;
        
        // TODO: move to ctr. parameter
        private readonly IPqlClientSecurityContext m_context = new PqlClientSecurityContext("context-1", "app", "tenant-1", "user-1");

        static PqlDataConnection()
        {
            var boundedCapacity = Environment.ProcessorCount * 16;
            
            ReaderStreams = new ObjectPool<BufferedReaderStream>(boundedCapacity, () => new BufferedReaderStream(84000));
            CommandStreams = new ObjectPool<CommandWriter>(boundedCapacity, () => new CommandWriter());
        }

        internal static ObjectPool<BufferedReaderStream> ReaderStreams { get; private set; }
        internal static ObjectPool<CommandWriter> CommandStreams { get; private set; }

        /// <summary>
        /// Ctr.
        /// </summary>
        public PqlDataConnection()
        {
            m_connectionProps = new ConnectionProps(null);
            m_connectionState = ConnectionState.Closed;
        }

        /// <summary>
        /// Gets or sets the string used to open a database.
        /// </summary>
        /// <returns>
        /// A string containing connection settings.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ConnectionString { get { return m_connectionProps.RawString; } set { m_connectionProps = new ConnectionProps(value); } }

        /// <summary>
        /// Parsed information from connection string, maybe some other context info about this connection.
        /// </summary>
        internal ConnectionProps ConnectionProps { get { return m_connectionProps; } }

        /// <summary>
        /// Gets the time to wait while trying to establish a connection before terminating the attempt and generating an error.
        /// </summary>
        /// <returns>
        /// The time (in seconds) to wait for a connection to open. The default value is 15 seconds.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int ConnectionTimeout { get { return m_connectionProps.ConnectionTimeoutSeconds; } }

        /// <summary>
        /// Gets the name of the current database or the database to be used after a connection is opened.
        /// </summary>
        /// <returns>
        /// The name of the current database or the name of the database to be used once a connection is open. The default value is an empty string.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string Database
        {
            get { return m_connectionProps.Database; }
        }

        /// <summary>
        /// Gets the name of the database server to which to connect.
        /// </summary>
        /// <returns>
        /// The name of the database server to which to connect. The default value is an empty string.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override string DataSource
        {
            get { return m_connectionProps.EndpointAddress.Host; }
        }

        /// <summary>
        /// Gets a string that represents the version of the server to which the object is connected.
        /// </summary>
        /// <returns>
        /// The version of the database. The format of the string returned depends on the specific type of connection you are using.
        /// </returns>
        /// <exception cref="T:System.InvalidOperationException"><see cref="P:System.Data.Common.DbConnection.ServerVersion"/> was called while 
        /// the returned Task was not completed and the connection was not opened after a call to OpenAsync.
        /// </exception><filterpriority>2</filterpriority>
        public override string ServerVersion
        {
            get { return m_connectionProps.ProtocolVersion; }
        }

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.ConnectionState"/> values.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override ConnectionState State
        {
            get { return m_connectionState; }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        protected override void Dispose(bool disposing)
        {
            Cleanup();
            base.Dispose(disposing);
        }

        /// <summary>
        /// Starts a database transaction.
        /// </summary>
        /// <returns>
        /// An object representing the new transaction.
        /// </returns>
        /// <param name="isolationLevel">Specifies the isolation level for the transaction.</param>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Closes the connection to the database.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public override void Close()
        {
            Cleanup();
        }

        /// <summary>
        /// Changes the current database for an open Connection object.
        /// </summary>
        /// <param name="databaseName">The name of the database to use in place of the current database. </param><filterpriority>2</filterpriority>
        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Creates and returns a <see cref="T:System.Data.Common.DbCommand"/> object associated with the current connection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Data.Common.DbCommand"/> object.
        /// </returns>
        protected override DbCommand CreateDbCommand()
        {
            return new PqlDataCommand(this);
        }

        /// <summary>
        /// Opens a database connection with the settings specified by the ConnectionString property of the provider-specific Connection object.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public override void Open()
        {
            if (m_connectionState == ConnectionState.Open || Environment.HasShutdownStarted)
            {
                return;
            }

            Cleanup();

            m_connectionState = ConnectionState.Connecting;
            try
            {
                m_channel = GetChannelFactory().CreateChannel(new EndpointAddress(m_connectionProps.EndpointAddress));

                if (m_connectionProps.ConnectionTimeoutSeconds == -1)
                {
                    ((IClientChannel) m_channel).Open(TimeSpan.MaxValue);
                }
                else
                {
                    ((IClientChannel) m_channel).Open(TimeSpan.FromSeconds(m_connectionProps.ConnectionTimeoutSeconds));
                }
            }
            catch
            {
                m_connectionState = ConnectionState.Broken;
                throw;
            }

            m_connectionState = ConnectionState.Open;
        }

        internal CancellationTokenSource CancellationTokenSource
        {
            get { return m_cancellationTokenSource; }
        }

        internal IPqlClientSecurityContext ClientSecurityContext { get { return m_context; } }

        internal IDataService BeginExecuteCommand(PqlDataCommand command)
        {
            if (m_activeCommand != null || m_connectionState == ConnectionState.Executing || m_connectionState == ConnectionState.Fetching)
            {
                throw new InvalidOperationException("Another command is being executed or fetching is in progress");
            }

            if (m_connectionState != ConnectionState.Open && m_connectionState != ConnectionState.Closed)
            {
                throw new InvalidOperationException("Cannot execute in this state: " + m_connectionState);
            }

            if (m_cancellationTokenSource != null && m_cancellationTokenSource.IsCancellationRequested)
            {
                m_connectionState = ConnectionState.Broken;
                throw new InvalidOperationException("Cancellation token source has not been reset to null");
            }

            Open();
            
            m_cancellationTokenSource = new CancellationTokenSource();
            m_connectionState = ConnectionState.Executing;
            m_activeCommand = command ?? throw new ArgumentNullException("command");
            return m_channel;
        }

        internal void SwitchToFetchingState()
        {
            if (m_activeCommand == null)
            {
                throw new InvalidOperationException("Current task is not set");
            }

            if (m_connectionState != ConnectionState.Executing)
            {
                throw new InvalidOperationException("Must be in executing state");
            }

            m_connectionState = ConnectionState.Fetching;
        }

        internal void ConfirmExecutionCompletion(bool successful)
        {
            if (m_activeCommand == null)
            {
                return;
            }

            m_connectionState = successful && (m_connectionState == ConnectionState.Executing || m_connectionState == ConnectionState.Fetching)
                ? ConnectionState.Open : ConnectionState.Broken;

            m_cancellationTokenSource = null;
            m_activeCommand = null;
        }

        private ChannelFactory<IDataService> GetChannelFactory()
        {
            if (m_channelFactory == null)
            {
                if (string.IsNullOrEmpty(ConnectionString))
                {
                    throw new InvalidOperationException("ConnectionString is not set");
                }

                m_connectionProps = new ConnectionProps(ConnectionString);

                var transport = new TcpTransportBindingElement
                    {
                        HostNameComparisonMode = HostNameComparisonMode.WeakWildcard,
                        TransferMode = TransferMode.Streamed,
                        ManualAddressing = true,
                        MaxReceivedMessageSize = long.MaxValue
                    };

                var binding = new CustomBinding(
                    new PqlMessageEncodingBindingElement(), transport)
                    {
                        OpenTimeout = TimeSpan.FromMinutes(1), 
                        CloseTimeout = TimeSpan.FromMinutes(1),
                        ReceiveTimeout = TimeSpan.FromMinutes(60),
                        SendTimeout = TimeSpan.FromMinutes(60)
                    };

                m_channelFactory = new ChannelFactory<IDataService>(binding);
            }

            return m_channelFactory;
        }

        private void Cleanup()
        {
            var source = Interlocked.CompareExchange(ref m_cancellationTokenSource, null, m_cancellationTokenSource);
            if (source != null)
            {
                // this will signal all commands and readers to abort any waiting
                source.Cancel();
            }

            m_connectionState = ConnectionState.Closed;
            m_activeCommand = null;

            var factory = Interlocked.CompareExchange(ref m_channelFactory, null, m_channelFactory);
            var channel = Interlocked.CompareExchange(ref m_channel, null, m_channel);
            
            RobustClose(factory);
            RobustClose((ICommunicationObject)channel);

            if (source != null)
            {
                source.Dispose();
            }
        }

        private static void RobustClose(ICommunicationObject channel)
        {
            if (channel == null || Environment.HasShutdownStarted)
            {
                return;
            }

            try
            {
                if (channel.State != CommunicationState.Faulted)
                {
                    channel.Close();
                }
            }
            catch (Exception)
            {
                if (Environment.HasShutdownStarted)
                {
                    // nodody cares now
                    return;
                }

                try
                {
                    if (channel.State != CommunicationState.Faulted)
                    {
                        channel.Abort();
                    }
                }
                catch {}
            }
        }
    }
}