using System.Data;
using System.Data.Common;

using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Protocol.Wire;

using PqlCall = Grpc.Core.AsyncDuplexStreamingCall<
            Pql.ClientDriver.Protocol.Wire.PqlRequestItem,
            Pql.ClientDriver.Protocol.Wire.PqlResponseItem>;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Command implementation for PQL server.
    /// </summary>
    internal sealed class PqlDataCommand : DbCommand, IPqlDbCommand
    {
        private CommandType _commandType;
        private PqlDataConnection _connection;
        private readonly PqlDataCommandParameterCollection _parameters;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="pqlDataConnection">Parent connection (optional). May be set later via <see cref="DbConnection"/> property</param>
        public PqlDataCommand(PqlDataConnection pqlDataConnection) : this()
        {
            _connection = pqlDataConnection;
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public PqlDataCommand()
        {
            _parameters = new PqlDataCommandParameterCollection();
            _commandType = CommandType.Text;
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        /// <exception cref="T:System.InvalidOperationException">The <see cref="P:System.Data.OleDb.OleDbCommand.Connection"/> is not set.
        /// -or- The <see cref="P:System.Data.OleDb.OleDbCommand.Connection"/> is not <see cref="M:System.Data.OleDb.OleDbConnection.Open"/>. </exception>
        /// <filterpriority>2</filterpriority>
        public override void Prepare()
        {
            var streamingCall = _connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(prepareOnly: true, returnDataset: false);

                using var response = SendCommand(streamingCall, dataRequest, CreateRequestParams(), null);
                using var reader = new PqlProtocolUtility(_connection, streamingCall);
                // this will throw if any server exceptions are reported
                reader.ReadResponse();
            }
            catch
            {
                _connection.ConfirmExecutionCompletion(false);
                throw;
            }
        }

        /// <summary>
        /// Attempts to cancels the execution of an <see cref="T:System.Data.IDbCommand"/>.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public override void Cancel()
        {

        }

        /// <summary>
        /// Creates a new instance of a <see cref="T:System.Data.Common.DbParameter"/> object.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Data.Common.DbParameter"/> object.
        /// </returns>
        protected override DbParameter CreateDbParameter() => new PqlDataCommandParameter();

        /// <summary>
        /// Executes the command text against the connection.
        /// </summary>
        /// <returns>
        /// A task representing the operation.
        /// </returns>
        /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior"/>.</param><exception cref="T:System.Data.Common.DbException">An error occurred while executing the command text.</exception><exception cref="T:System.ArgumentException">An invalid <see cref="T:System.Data.CommandBehavior"/> value.</exception>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var pqlCall = _connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(false, !behavior.HasFlag(CommandBehavior.SchemaOnly));

                var response = SendCommand(pqlCall, dataRequest, CreateRequestParams(), null);
                return new PqlDataReader(_connection, pqlCall, response);
            }
            catch
            {
                _connection.ConfirmExecutionCompletion(false);
                throw;
            }
        }

        /// <summary>
        /// Overload of <see cref="IDbCommand.ExecuteNonQuery"/> for bulk operations.
        /// </summary>
        /// <param name="argCount">Number of items in <paramref name="bulkArgs"/></param>
        /// <param name="bulkArgs">Data to be streamed to server, one row at a time. May be same object with different values. 
        /// Must match fields number and order specified by <paramref name="fieldNames"/></param>
        /// <param name="entityName">Name of the entity to be run operation on</param>
        /// <param name="fieldNames">Names of fields to be selected, inserted or updated. Must include primary key field for all modification commands, optional for select</param>
        /// <returns>Number of records selected or affected</returns>
        /// <exception cref="ArgumentNullException"><paramref name="bulkArgs"/> cannot be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="fieldNames"/> cannot be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="entityName"/> cannot be null</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="argCount"/> has invalid value</exception>
        public int BulkInsert(string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> bulkArgs) => ExecuteNonQuery(StatementType.Insert, entityName, fieldNames, argCount, bulkArgs);

        /// <summary>
        /// Overload of <see cref="IDbCommand.ExecuteNonQuery"/> for bulk operations.
        /// </summary>
        /// <param name="argCount">Number of items in <paramref name="requestBulk"/></param>
        /// <param name="requestBulk">Data to be streamed to server, one row at a time. May be same object with different values. 
        /// Must match fields number and order specified by <paramref name="fieldNames"/></param>
        /// <param name="entityName">Name of the entity to be run operation on</param>
        /// <param name="fieldNames">Names of fields to be selected, inserted or updated. Must include primary key field for all modification commands, optional for select</param>
        /// <returns>Number of records selected or affected</returns>
        /// <exception cref="ArgumentNullException"><paramref name="requestBulk"/> cannot be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="fieldNames"/> cannot be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="entityName"/> cannot be null</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="argCount"/> has invalid value</exception>
        public int BulkUpdate(string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> requestBulk) => ExecuteNonQuery(StatementType.Update, entityName, fieldNames, argCount, requestBulk);

        private int ExecuteNonQuery(StatementType statementType, string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> requestBulk)
        {
            var pqlCall = _connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(false, false);
                var dataRequestBulk = requestBulk == null ? null : CreateRequestBulk(statementType, entityName, fieldNames, argCount, requestBulk);

                using var response = SendCommand(pqlCall, dataRequest, CreateRequestParams(), dataRequestBulk);
                return PqlProtocolUtility.ReadResponseHeaders(pqlCall).GetAwaiter().GetResult().RecordsAffected;
            }
            catch
            {
                _connection.ConfirmExecutionCompletion(false);
                throw;
            }
        }

        /// <summary>
        /// Executes an SQL statement against the Connection object of a .NET Framework data provider, and returns the number of rows affected.
        /// </summary>
        /// <returns>
        /// The number of rows affected.
        /// </returns>
        public override int ExecuteNonQuery()
        {
            var pqlCall = _connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(false, false);

                var response = SendCommand(pqlCall, dataRequest, CreateRequestParams(), null).ConfigureAwait(false).GetAwaiter().GetResult();
                return PqlProtocolUtility.ReadResponseHeaders(pqlCall).GetAwaiter().GetResult().RecordsAffected;
            }
            catch
            {
                _connection.ConfirmExecutionCompletion(false);
                throw;
            }
        }

        private async Task<DataResponse> SendCommand(PqlCall pqlCall, DataRequest dataRequest, DataRequestParams? dataRequestParams, DataRequestBulk? dataRequestBulk)
        {
            await pqlCall.RequestStream.WriteAsync(new PqlRequestItem { Header = dataRequest });
            if (dataRequestParams is not null)
            {
                await pqlCall.RequestStream.WriteAsync(new PqlRequestItem { ParamsHeader = dataRequestParams });
                foreach (PqlDataCommandParameter p in dataRequestParams.Bulk)
                {
                    using var stream = new MemoryStream();
                    using var writer = new BinaryWriter(stream);
                    p.Write(writer);
                    writer.Flush();
                    stream.Position = 0;

                    await pqlCall.RequestStream.WriteAsync(
                        new PqlRequestItem { ParamsRow = Google.Protobuf.ByteString.FromStream(stream) });
                }
            }

            if (dataRequestBulk is not null)
            {
                await pqlCall.RequestStream.WriteAsync(new PqlRequestItem { BulkHeader = dataRequestBulk });
                foreach (var row in dataRequestBulk.Bulk)
                {
                    using var stream = new MemoryStream();
                    using var writer = new BinaryWriter(stream);
                    row.Write(writer);
                    writer.Flush();
                    stream.Position = 0;

                    await pqlCall.RequestStream.WriteAsync(
                        new PqlRequestItem { BulkRow = Google.Protobuf.ByteString.FromStream(stream) });
                }
            }

            if (!await pqlCall.ResponseStream.MoveNext(CancellationToken.None))
            {
                throw new DataException("Failed to retrieve any response");
            }

            if (pqlCall.ResponseStream.Current.ItemCase != PqlResponseItem.ItemOneofCase.Header)
            {
                throw new DataException("Server didn't send a proper response header");
            }

            return pqlCall.ResponseStream.Current.Header;
        }

        private DataRequest CreateRequest(bool prepareOnly, bool returnDataset)
        {
            var authContext = _connection.ClientSecurityContext
                ?? throw new InvalidOperationException("Authentication context is not set on the thread");

            return new DataRequest
            {
                ScopeId = _connection.ConnectionProps.ScopeId,
                Created = DateTime.UtcNow,
                CommandText = CommandText,
                PrepareOnly = prepareOnly,
                ReturnDataset = returnDataset,
                Auth = new AuthenticationContext
                {
                    ApplicationName = authContext.ApplicationName,
                    ContextId = authContext.ContextId,
                    TenantId = authContext.TenantId,
                    UserId = authContext.UserId
                },
            };
        }

        private DataRequestBulk CreateRequestBulk(StatementType statementType, string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> bulkArgs)
        {
            if (string.IsNullOrEmpty(entityName))
            {
                throw new ArgumentNullException(nameof(entityName));
            }

            if (fieldNames == null || fieldNames.Length == 0)
            {
                throw new ArgumentNullException(nameof(fieldNames));
            }

            if (bulkArgs == null)
            {
                throw new ArgumentNullException(nameof(bulkArgs));
            }

            if (statementType is not StatementType.Insert and not StatementType.Update)
            {
                throw new ArgumentOutOfRangeException(nameof(statementType), statementType, "Only insert and update bulk statements are supported");
            }

            if (argCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(argCount), argCount, "argCount cannot be negative");
            }

            return new DataRequestBulk
            {
                DbStatementType = statementType,
                EntityName = entityName,
                FieldNames = { fieldNames },
                InputItemsCount = argCount,
                Bulk = bulkArgs
            };
        }

        private DataRequestParams? CreateRequestParams()
        {
            _parameters.Validate();

            if (_parameters.Count == 0)
            {
                return null;
            }

            var arr = _parameters.ParametersData;

            var result = new DataRequestParams
            {
                DataTypes = { new int[arr.Count] },
                Names = { new string[arr.Count] },
                IsCollectionFlags = { new int[BitVector.GetArrayLength(arr.Count)] }
            };

            for (var i = 0; i < arr.Count; i++)
            {
                result.DataTypes[i] = (int)arr[i].DbType;
                result.Names[i] = arr[i].ParameterName;
                if (arr[i].IsValidatedCollection)
                {
                    BitVector.Set(result.IsCollectionFlags, i);
                }
            }

            result.Bulk = _parameters;

            return result;
        }

        /// <summary>
        /// Executes the query, and returns the first column of the first row in the resultset returned by the query. Extra columns or rows are ignored.
        /// </summary>
        /// <returns>
        /// The first column of the first row in the resultset.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override object ExecuteScalar()
        {
            using var reader = ExecuteDbDataReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);
            if (reader == null || !reader.Read() || reader.FieldCount < 1)
            {
                throw new DataException("Could not retrieve any rows");
            }

            return reader.GetValue(0);
        }

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.Common.DbConnection"/> used by this <see cref="T:System.Data.Common.DbCommand"/>.
        /// </summary>
        /// <returns>
        /// The connection to the data source.
        /// </returns>
        protected override DbConnection DbConnection
        {
            get => _connection;
            set => _connection = (PqlDataConnection)value;
        }

        /// <summary>
        /// Gets the collection of <see cref="T:System.Data.Common.DbParameter"/> objects.
        /// </summary>
        /// <returns>
        /// The parameters of the SQL statement or stored procedure.
        /// </returns>
        protected override DbParameterCollection DbParameterCollection => _parameters;

        /// <summary>
        /// Gets or sets the <see cref="P:System.Data.Common.DbCommand.DbTransaction"/> within which this <see cref="T:System.Data.Common.DbCommand"/> object executes.
        /// </summary>
        /// <returns>
        /// The transaction within which a Command object of a .NET Framework data provider executes. The default value is a null reference (Nothing in Visual Basic).
        /// </returns>
        protected override DbTransaction DbTransaction { get => null; set { } }

        /// <summary>
        /// Gets or sets a value indicating whether the command object should be visible in a customized interface control.
        /// </summary>
        /// <returns>
        /// true, if the command object should be visible in a control; otherwise false. The default is true.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool DesignTimeVisible { get; set; }

        /// <summary>
        /// Gets or sets the text command to run against the data source.
        /// </summary>
        /// <returns>
        /// The text command to execute. The default value is an empty string ("").
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string CommandText { get; set; }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        /// <returns>
        /// The time (in seconds) to wait for the command to execute. The default value is 30 seconds.
        /// </returns>
        /// <exception cref="T:System.ArgumentException">The property value assigned is less than 0. </exception><filterpriority>2</filterpriority>
        public override int CommandTimeout { get; set; }

        /// <summary>
        /// Indicates or specifies how the <see cref="P:System.Data.IDbCommand.CommandText"/> property is interpreted.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.CommandType"/> values. The default is Text.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                if (value != CommandType.Text)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), value, "Only text commands are supported at this time");
                }

                _commandType = value;
            }
        }

        /// <summary>
        /// Not supported by PQL server.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.UpdateRowSource"/> values. The default is Both unless the command is automatically generated. Then the default is None.
        /// </returns>
        /// <exception cref="T:System.ArgumentException">The value entered was not one of the <see cref="T:System.Data.UpdateRowSource"/> values. </exception><filterpriority>2</filterpriority>
        public override UpdateRowSource UpdatedRowSource
        {
            get => UpdateRowSource.None;
            set { }
        }
    }
}