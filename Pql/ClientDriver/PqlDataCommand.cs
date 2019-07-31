using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.ServiceModel.Channels;
using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Wcf;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Command implementation for PQL server.
    /// </summary>
    internal sealed class PqlDataCommand : DbCommand, IPqlDbCommand
    {
        private CommandType m_commandType;
        private PqlDataConnection m_connection;
        private readonly PqlDataCommandParameterCollection m_parameters;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="pqlDataConnection">Parent connection (optional). May be set later via <see cref="DbConnection"/> property</param>
        public PqlDataCommand(PqlDataConnection pqlDataConnection) : this()
        {
            m_connection = pqlDataConnection;
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public PqlDataCommand()
        {
            m_parameters = new PqlDataCommandParameterCollection();
            m_commandType = CommandType.Text;
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        /// <exception cref="T:System.InvalidOperationException">The <see cref="P:System.Data.OleDb.OleDbCommand.Connection"/> is not set.
        /// -or- The <see cref="P:System.Data.OleDb.OleDbCommand.Connection"/> is not <see cref="M:System.Data.OleDb.OleDbConnection.Open"/>. </exception>
        /// <filterpriority>2</filterpriority>
        public override void Prepare()
        {
            var channel = m_connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(true, false);
                
                using (var response = SendCommand(channel, dataRequest, CreateRequestParams(), null))
                {
                    var streamHolder = PqlDataConnection.ReaderStreams.Take(m_connection.CancellationTokenSource.Token);
                    try
                    {
                        streamHolder.Item.Attach(response.Stream);
                        using (var reader = new PqlProtocolUtility(m_connection, streamHolder.Item, streamHolder))
                        {
                            // this will throw if any server exceptions are reported
                            reader.ReadResponse();
                        }
                    }
                    catch
                    {
                        streamHolder.Dispose();
                        throw;
                    }
                }
            }
            catch
            {
                m_connection.ConfirmExecutionCompletion(false);
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
        protected override DbParameter CreateDbParameter()
        {
            return new PqlDataCommandParameter();
        }

        /// <summary>
        /// Executes the command text against the connection.
        /// </summary>
        /// <returns>
        /// A task representing the operation.
        /// </returns>
        /// <param name="behavior">An instance of <see cref="T:System.Data.CommandBehavior"/>.</param><exception cref="T:System.Data.Common.DbException">An error occurred while executing the command text.</exception><exception cref="T:System.ArgumentException">An invalid <see cref="T:System.Data.CommandBehavior"/> value.</exception>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var channel = m_connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(false, !behavior.HasFlag(CommandBehavior.SchemaOnly));

                var response = SendCommand(channel, dataRequest, CreateRequestParams(), null);
                var streamHolder = PqlDataConnection.ReaderStreams.Take(m_connection.CancellationTokenSource.Token);
                try
                {
                    streamHolder.Item.Attach(response.Stream);
                    return new PqlDataReader(m_connection, streamHolder.Item, streamHolder, response);
                }
                catch
                {
                    streamHolder.Dispose();
                    response.Close();
                    throw;
                }
            }
            catch
            {
                m_connection.ConfirmExecutionCompletion(false);
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
        public int BulkInsert(string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> bulkArgs)
        {
            return ExecuteNonQuery(StatementType.Insert, entityName, fieldNames, argCount, bulkArgs);
        }
        
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
        public int BulkUpdate(string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> requestBulk)
        {
            return ExecuteNonQuery(StatementType.Update, entityName, fieldNames, argCount, requestBulk);
        }

        private int ExecuteNonQuery(StatementType statementType, string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> requestBulk)
        {
            var channel = m_connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(false, false);
                var dataRequestBulk = requestBulk == null ? null : CreateRequestBulk(statementType, entityName, fieldNames, argCount, requestBulk);

                using (var response = SendCommand(channel, dataRequest, CreateRequestParams(), dataRequestBulk))
                {
                    var streamHolder = PqlDataConnection.ReaderStreams.Take(m_connection.CancellationTokenSource.Token);
                    try
                    {
                        streamHolder.Item.Attach(response.Stream);
                        using (var reader = new PqlProtocolUtility(m_connection, streamHolder.Item, streamHolder))
                        {
                            // this will throw if any server exceptions are reported
                            return reader.ReadResponse().RecordsAffected;
                        }
                    }
                    catch
                    {
                        streamHolder.Dispose();
                        throw;
                    }
                }
            }
            catch
            {
                m_connection.ConfirmExecutionCompletion(false);
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
            var channel = m_connection.BeginExecuteCommand(this);
            try
            {
                var dataRequest = CreateRequest(false, false);

                using (var response = SendCommand(channel, dataRequest, CreateRequestParams(), null))
                {
                    var streamHolder = PqlDataConnection.ReaderStreams.Take(m_connection.CancellationTokenSource.Token);
                    try
                    {
                        streamHolder.Item.Attach(response.Stream);
                        using (var reader = new PqlProtocolUtility(m_connection, streamHolder.Item, streamHolder))
                        {
                            // this will throw if any server exceptions are reported
                            return reader.ReadResponse().RecordsAffected;
                        }
                    }
                    catch
                    {
                        streamHolder.Dispose();
                        throw;
                    }
                }
            }
            catch
            {
                m_connection.ConfirmExecutionCompletion(false);
                throw;
            }
        }

        private PqlMessage SendCommand(IDataService channel, DataRequest dataRequest, DataRequestParams dataRequestParams, DataRequestBulk dataRequestBulk)
        {
            var authContext = m_connection.ClientSecurityContext;

            if (authContext == null)
            {
                throw new InvalidOperationException("Authentication context is not set on the thread");
            }

            if (string.IsNullOrWhiteSpace(authContext.TenantId))
            {
                throw new InvalidOperationException("Current authentication context does not have value for TenantId");
            }

            Message responseMessage;
            using (var holder = PqlDataConnection.CommandStreams.Take(m_connection.CancellationTokenSource.Token))
            {
                holder.Item.Attach(dataRequest, dataRequestParams, dataRequestBulk);

                var requestMessage = new PqlMessage(
                    holder.Item, new IDisposable[] {holder},
                    AuthContextSerializer.GetString(authContext),
                    m_connection.ConnectionProps.ScopeId,
                    m_connection.ConnectionProps.ProtocolVersion);

                responseMessage = channel.Process(requestMessage);
            }


            if (responseMessage is PqlMessage pqlMessage)
            {
                return pqlMessage;
            }

            throw new DataException(string.Format(
                "Message must be of type {0}. Actual type that came from WCF transport was {1}", 
                typeof(PqlMessage).AssemblyQualifiedName,
                responseMessage.GetType().AssemblyQualifiedName));
        }

        private DataRequest CreateRequest(bool prepareOnly, bool returnDataset)
        {
            return new DataRequest
            {
                CommandText = CommandText,
                PrepareOnly = prepareOnly,
                ReturnDataset = returnDataset
            };
        }

        private DataRequestBulk CreateRequestBulk(StatementType statementType, string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> bulkArgs)
        {
            if (string.IsNullOrEmpty(entityName))
            {
                throw new ArgumentNullException("entityName");
            }

            if (fieldNames == null || fieldNames.Length == 0)
            {
                throw new ArgumentNullException("fieldNames");
            }

            if (bulkArgs == null)
            {
                throw new ArgumentNullException("bulkArgs");
            }

            if (statementType != StatementType.Insert && statementType != StatementType.Update)
            {
                throw new ArgumentOutOfRangeException("statementType", statementType, "Only insert and update bulk statements are supported");
            }

            if (argCount < 0)
            {
                throw new ArgumentOutOfRangeException("argCount", argCount, "argCount cannot be negative");
            }

            return new DataRequestBulk
                {
                    DbStatementType = statementType,
                    EntityName = entityName,
                    FieldNames = fieldNames,
                    InputItemsCount = argCount,
                    Bulk = bulkArgs
                };
        }

        private DataRequestParams CreateRequestParams()
        {
            m_parameters.Validate();

            if (m_parameters.Count == 0)
            {
                return null;
            }

            var arr = m_parameters.ParametersData;

            var result = new DataRequestParams
                {
                    DataTypes = new DbType[arr.Count],
                    Names = new string[arr.Count],
                    IsCollectionFlags = new int[BitVector.GetArrayLength(arr.Count)]
                };

            for (var i = 0; i < arr.Count; i++)
            {
                result.DataTypes[i] = arr[i].DbType;
                result.Names[i] = arr[i].ParameterName;
                if (arr[i].IsValidatedCollection)
                {
                    BitVector.Set(result.IsCollectionFlags, i);
                }
            }

            result.Bulk = m_parameters;

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
            using (var reader = ExecuteDbDataReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow))
            {
                if (reader == null || !reader.Read() || reader.FieldCount < 1)
                {
                    throw new DataException("Could not retrieve any rows");
                }

                return reader.GetValue(0);
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.Common.DbConnection"/> used by this <see cref="T:System.Data.Common.DbCommand"/>.
        /// </summary>
        /// <returns>
        /// The connection to the data source.
        /// </returns>
        protected override DbConnection DbConnection
        {
            get { return m_connection; }
            set { m_connection = (PqlDataConnection)value; }
        }

        /// <summary>
        /// Gets the collection of <see cref="T:System.Data.Common.DbParameter"/> objects.
        /// </summary>
        /// <returns>
        /// The parameters of the SQL statement or stored procedure.
        /// </returns>
        protected override DbParameterCollection DbParameterCollection
        {
            get { return m_parameters; }
        }

        /// <summary>
        /// Gets or sets the <see cref="P:System.Data.Common.DbCommand.DbTransaction"/> within which this <see cref="T:System.Data.Common.DbCommand"/> object executes.
        /// </summary>
        /// <returns>
        /// The transaction within which a Command object of a .NET Framework data provider executes. The default value is a null reference (Nothing in Visual Basic).
        /// </returns>
        protected override DbTransaction DbTransaction { get {return null;}  set {}}

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
            get { return m_commandType; }
            set
            {
                if (value != CommandType.Text)
                {
                    throw new ArgumentOutOfRangeException("value", value, "Only text commands are supported at this time");
                }
                m_commandType = value;
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
            get { return UpdateRowSource.None; }
            set { }
        }
    }
}