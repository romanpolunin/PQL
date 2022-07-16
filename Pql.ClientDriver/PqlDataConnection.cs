using System.Data;
using System.Data.Common;

using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;

using PqlCall = Grpc.Core.AsyncDuplexStreamingCall<
            Pql.ClientDriver.Protocol.Wire.PqlRequestItem,
            Pql.ClientDriver.Protocol.Wire.PqlResponseItem>;


namespace Pql.ClientDriver
{
    /// <summary>
    /// Implements a connection to PQL server.
    /// </summary>
    public sealed class PqlDataConnection : DbConnection
    {
        private ConnectionProps _connectionProps;
        private GrpcChannel _channel;
        private Protocol.Wire.PqlService.PqlServiceClient _serviceClient;
        private PqlCall _pqlCall;
        private ConnectionState _connectionState;
        private PqlDataCommand _activeCommand;
        private CancellationTokenSource _cancellationTokenSource;

        public static Protocol.Wire.PqlService.PqlServiceClient CreateClient(GrpcChannel channel) => new (channel);

        /// <summary>
        /// Ctr.
        /// </summary>
        public PqlDataConnection()
        {
            _connectionProps = new ConnectionProps(null);
            _connectionState = ConnectionState.Closed;
        }

        /// <summary>
        /// Gets or sets the string used to open a database.
        /// </summary>
        /// <returns>
        /// A string containing connection settings.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string? ConnectionString
        {
            get => _connectionProps.RawString;
            set => _connectionProps = new ConnectionProps(value);
        }

        /// <summary>
        /// Parsed information from connection string, maybe some other context info about this connection.
        /// </summary>
        internal ConnectionProps ConnectionProps => _connectionProps;

        /// <summary>
        /// Gets the time to wait while trying to establish a connection before terminating the attempt and generating an error.
        /// </summary>
        /// <returns>
        /// The time (in seconds) to wait for a connection to open. The default value is 15 seconds.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int ConnectionTimeout => _connectionProps.ConnectionTimeoutSeconds;

        /// <summary>
        /// Gets the name of the current database or the database to be used after a connection is opened.
        /// </summary>
        /// <returns>
        /// The name of the current database or the name of the database to be used once a connection is open. The default value is an empty string.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string Database => _connectionProps.Database;

        /// <summary>
        /// Gets the name of the database server to which to connect.
        /// </summary>
        /// <returns>
        /// The name of the database server to which to connect. The default value is an empty string.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override string DataSource => _connectionProps.EndpointAddress.Host;

        /// <summary>
        /// Gets a string that represents the version of the server to which the object is connected.
        /// </summary>
        /// <returns>
        /// The version of the database. The format of the string returned depends on the specific type of connection you are using.
        /// </returns>
        /// <exception cref="T:System.InvalidOperationException"><see cref="P:System.Data.Common.DbConnection.ServerVersion"/> was called while 
        /// the returned Task was not completed and the connection was not opened after a call to OpenAsync.
        /// </exception><filterpriority>2</filterpriority>
        public override string ServerVersion => _connectionProps.ProtocolVersion;

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.ConnectionState"/> values.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override ConnectionState State => _connectionState;

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        protected override void Dispose(bool disposing)
        {
            CleanupAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            base.Dispose(disposing);
        }

        /// <summary>
        /// Starts a database transaction.
        /// </summary>
        /// <returns>
        /// An object representing the new transaction.
        /// </returns>
        /// <param name="isolationLevel">Specifies the isolation level for the transaction.</param>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        /// <summary>
        /// Closes the connection to the database.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public override void Close() => CleanupAsync().ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>
        /// Changes the current database for an open Connection object.
        /// </summary>
        /// <param name="databaseName">The name of the database to use in place of the current database. </param><filterpriority>2</filterpriority>
        public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();

        /// <summary>
        /// Creates and returns a <see cref="T:System.Data.Common.DbCommand"/> object 
        /// associated with the current connection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Data.Common.DbCommand"/> object.
        /// </returns>
        protected override DbCommand CreateDbCommand() => new PqlDataCommand(this);

        /// <summary>
        /// Opens a database connection with the settings specified by the ConnectionString property of the provider-specific Connection object.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public override void Open()
        {
            if (_connectionState == ConnectionState.Open || Environment.HasShutdownStarted)
            {
                return;
            }

            CleanupAsync().ConfigureAwait(false).GetAwaiter().GetResult();

            _connectionState = ConnectionState.Connecting;
            try
            {
                _channel = GrpcChannel.ForAddress(_connectionProps.EndpointAddress,
                    new GrpcChannelOptions
                    {
                        ServiceConfig = new ServiceConfig
                        {

                        }
                    });

                _channel.ConnectAsync().GetAwaiter().GetResult();

                _serviceClient = new Protocol.Wire.PqlService.PqlServiceClient(_channel);

                /*if (_connectionProps.ConnectionTimeoutSeconds == -1)
                {
                    _channel.Open(TimeSpan.MaxValue);
                }
                else
                {
                    ((IClientChannel) _channel).Open(TimeSpan.FromSeconds(_connectionProps.ConnectionTimeoutSeconds));
                }*/
            }
            catch
            {
                _connectionState = ConnectionState.Broken;
                throw;
            }

            _connectionState = ConnectionState.Open;
        }

        internal CancellationTokenSource CancellationTokenSource => _cancellationTokenSource;

        internal IPqlClientSecurityContext ClientSecurityContext { get; } = new PqlClientSecurityContext("context-1", "app", "tenant-1", "user-1");

        internal PqlCall BeginExecuteCommand(PqlDataCommand command)
        {
            if (_activeCommand != null || _connectionState == ConnectionState.Executing || _connectionState == ConnectionState.Fetching)
            {
                throw new InvalidOperationException("Another command is being executed or fetching is in progress");
            }

            if (_connectionState is not ConnectionState.Open and not ConnectionState.Closed)
            {
                throw new InvalidOperationException("Cannot execute in this state: " + _connectionState);
            }

            if (_cancellationTokenSource != null && _cancellationTokenSource.IsCancellationRequested)
            {
                _connectionState = ConnectionState.Broken;
                throw new InvalidOperationException("Cancellation token source has not been reset to null");
            }

            Open();

            _cancellationTokenSource = new CancellationTokenSource();
            _pqlCall = _serviceClient.Request(cancellationToken: _cancellationTokenSource.Token);
            _connectionState = ConnectionState.Executing;
            _activeCommand = command ?? throw new ArgumentNullException(nameof(command));
            return _pqlCall;
        }

        internal void SwitchToFetchingState()
        {
            if (_activeCommand == null)
            {
                throw new InvalidOperationException("Current task is not set");
            }

            if (_connectionState != ConnectionState.Executing)
            {
                throw new InvalidOperationException("Must be in executing state");
            }

            _connectionState = ConnectionState.Fetching;
        }

        internal void ConfirmExecutionCompletion(bool successful)
        {
            if (_activeCommand == null)
            {
                return;
            }

            _connectionState = successful && (_connectionState == ConnectionState.Executing || _connectionState == ConnectionState.Fetching)
                ? ConnectionState.Open : ConnectionState.Broken;

            _cancellationTokenSource = null;
            _activeCommand = null;
        }

        private async Task CleanupAsync()
        {
            var source = Interlocked.CompareExchange(ref _cancellationTokenSource, null, _cancellationTokenSource);
            if (source != null)
            {
                // this will signal all commands and readers to abort any waiting
                source.Cancel();
            }

            _connectionState = ConnectionState.Closed;
            _activeCommand = null;

            var streamingCall = Interlocked.CompareExchange(ref _pqlCall, null, _pqlCall);
            if (streamingCall is not null)
            {
                await streamingCall.RequestStream.CompleteAsync();
                streamingCall.Dispose();
            }

            if (source != null)
            {
                source.Dispose();
            }
        }
    }
}