using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

using Pql.ClientDriver.Protocol;
using Pql.ClientDriver.Protocol.Wire;

using PqlCall = Grpc.Core.AsyncDuplexStreamingCall<
            Pql.ClientDriver.Protocol.Wire.PqlRequestItem,
            Pql.ClientDriver.Protocol.Wire.PqlResponseItem>;

namespace Pql.ClientDriver
{
    /// <summary>
    /// IDataReader implementation for PQL query processor client driver.
    /// </summary>
    internal sealed class PqlDataReader : DbDataReader
    {
        //private volatile DataTable m_schemaTable;
        private readonly DataResponse _scheme;
        private readonly RowData _currentRow;
        private readonly BinaryReader _reader;
        private readonly PqlProtocolUtility _protocolUtility;
        private readonly PqlCall _pqlCall;
        private ReaderState _state;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="connection">Parent connection</param>
        /// <param name="inputStream">Incoming data stream from server</param>
        /// <param name="holders">Array of holder objects that must be disposed when reader is closed</param>
        public PqlDataReader(PqlDataConnection connection, PqlCall pqlCall, params IDisposable[] holders)
        {
            _protocolUtility = new PqlProtocolUtility(connection, pqlCall);
            try
            {
                _scheme = _protocolUtility.ReadResponse().ConfigureAwait(false).GetAwaiter().GetResult();
                
                if (_scheme.Fields != null && _scheme.Fields.Count > 0)
                {
                    _currentRow = new RowData(_scheme.Fields.Select(x => x.DataType).ToArray());
                    _state = ReaderState.New;
                }
                else
                {
                    _state = ReaderState.Closed;
                }

                connection.SwitchToFetchingState();
            }
            catch
            {
                _protocolUtility.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Set to true to close the connection when reader is disposed or closed.
        /// </summary>
        public bool CloseConnection { get; set; }

        /// <summary>
        /// Releases the managed resources used by the <see cref="T:System.Data.Common.DbDataReader"/> and optionally releases the unmanaged resources.
        /// </summary>
        /// <param name="disposing">true to release managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            _state = ReaderState.Closed;

            if (_protocolUtility != null)
            {
                var conn = _protocolUtility.Connection;

                try
                {
                    _protocolUtility.Dispose();
                }
                finally
                {
                    if (conn != null && CloseConnection)
                    {
                        conn.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Gets the name for the field to find.
        /// </summary>
        /// <returns>
        /// The name of the field or the empty string (""), if there is no value to return.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override string GetName(int i) => _scheme.GetName(i);

        /// <summary>
        /// Gets the data type information for the specified field.
        /// </summary>
        /// <returns>
        /// The data type information for the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override string GetDataTypeName(int i) => _scheme.RequireField(i).DataType.ToString();

        /// <summary>
        /// Returns an <see cref="T:System.Collections.IEnumerator"/> that can be used to iterate through the rows in the data reader.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> that can be used to iterate through the rows in the data reader.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();

        /// <summary>
        /// Gets the <see cref="T:System.Type"/> information corresponding to the type of <see cref="T:System.Object"/> that would be returned from <see cref="M:System.Data.IDataRecord.GetValue(System.Int32)"/>.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.Type"/> information corresponding to the type of <see cref="T:System.Object"/> that would be returned from <see cref="M:System.Data.IDataRecord.GetValue(System.Int32)"/>.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override Type GetFieldType(int i) => RowData.DeriveSystemType(_scheme.RequireField(i).DataType);

        /// <summary>
        /// Return the value of the specified field.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.Object"/> which will contain the field value upon return.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override object GetValue(int i) => CurrentRow.GetValue(i);

        /// <summary>
        /// Populates an array of objects with the column values of the current record.
        /// </summary>
        /// <returns>
        /// The number of instances of <see cref="T:System.Object"/> in the array.
        /// </returns>
        /// <param name="values">An array of <see cref="T:System.Object"/> to copy the attribute fields into. </param>
        /// <filterpriority>2</filterpriority>        
        public override int GetValues(object[] values) => CurrentRow.GetValues(values);

        /// <summary>
        /// Return the index of the named field.
        /// </summary>
        /// <returns>
        /// The index of the named field.
        /// </returns>
        /// <param name="name">The name of the field to find. </param><filterpriority>2</filterpriority>        
        public override int GetOrdinal(string name) => _scheme.RequireField(name).Ordinal;

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <returns>
        /// The value of the column.
        /// </returns>
        /// <param name="i">The zero-based column ordinal. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override bool GetBoolean(int i) => CurrentRow.GetBoolean(i);

        /// <summary>
        /// Gets the 8-bit unsigned integer value of the specified column.
        /// </summary>
        /// <returns>
        /// The 8-bit unsigned integer value of the specified column.
        /// </returns>
        /// <param name="i">The zero-based column ordinal. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override byte GetByte(int i) => CurrentRow.GetByte(i);

        /// <summary>
        /// Reads a stream of bytes from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <returns>
        /// The actual number of bytes read.
        /// </returns>
        /// <param name="i">The zero-based column ordinal. </param>
        /// <param name="fieldoffset">The index within the field from which to start the read operation. </param>
        /// <param name="buffer">The buffer into which to read the stream of bytes. </param>
        /// <param name="bufferoffset">The index for <paramref name="buffer"/> to start the read operation. </param>
        /// <param name="length">The number of bytes to read. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override long GetBytes(int i, long fieldoffset, byte[]? buffer, int bufferoffset, int length) => CurrentRow.GetBinary(i, fieldoffset, buffer, bufferoffset, length);

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <returns>
        /// The character value of the specified column.
        /// </returns>
        /// <param name="i">The zero-based column ordinal. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override char GetChar(int i) => CurrentRow.GetChar(i);

        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer as an array, starting at the given buffer offset.
        /// </summary>
        /// <returns>
        /// The actual number of characters read.
        /// </returns>
        /// <param name="i">The zero-based column ordinal. </param><param name="fieldoffset">The index within the row from which to start the read operation. </param>
        /// <param name="buffer">The buffer into which to read the stream of bytes. </param>
        /// <param name="bufferoffset">The index for <paramref name="buffer"/> to start the read operation. </param>
        /// <param name="length">The number of bytes to read. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => CurrentRow.GetChars(i, fieldoffset, buffer, bufferoffset, length);

        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <returns>
        /// The GUID value of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override Guid GetGuid(int i) => CurrentRow.GetGuid(i);

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <returns>
        /// The 16-bit signed integer value of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override short GetInt16(int i) => CurrentRow.GetInt16(i);

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <returns>
        /// The 32-bit signed integer value of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override int GetInt32(int i) => CurrentRow.GetInt32(i);

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <returns>
        /// The 64-bit signed integer value of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override long GetInt64(int i) => CurrentRow.GetInt64(i);

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <returns>
        /// The single-precision floating point number of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override float GetFloat(int i) => CurrentRow.GetFloat(i);

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <returns>
        /// The double-precision floating point number of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override double GetDouble(int i) => CurrentRow.GetDouble(i);

        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <returns>
        /// The string value of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override string GetString(int i) => CurrentRow.GetString(i);

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <returns>
        /// The fixed-position numeric value of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override decimal GetDecimal(int i) => CurrentRow.GetCurrency(i);

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <returns>
        /// The date and time data value of the specified field.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override DateTime GetDateTime(int i) => CurrentRow.GetDateTime(i);

        /// <summary>
        /// Return whether the specified field is set to null.
        /// </summary>
        /// <returns>
        /// true if the specified field is set to null; otherwise, false.
        /// </returns>
        /// <param name="i">The index of the field to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>        
        public override bool IsDBNull(int i) => !BitVector.Get(CurrentRow.NotNulls, i);

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        /// <returns>
        /// When not positioned in a valid recordset, 0; otherwise, the number of columns in the current record. The default is -1.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int FieldCount => _scheme.Fields.Count;

        /// <summary>
        /// Gets a value that indicates whether this <see cref="T:System.Data.Common.DbDataReader"/> contains one or more rows.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Data.Common.DbDataReader"/> contains one or more rows; otherwise false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool HasRows => _state is ReaderState.New or ReaderState.Fetching;

        /// <summary>
        /// Gets the column located at the specified index.
        /// </summary>
        /// <returns>
        /// The column located at the specified index as an <see cref="T:System.Object"/>.
        /// </returns>
        /// <param name="i">The zero-based index of the column to get. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="P:System.Data.IDataRecord.FieldCount"/>. </exception>
        /// <filterpriority>2</filterpriority>
        public override object this[int i] => CurrentRow.GetValue(i);

        /// <summary>
        /// Gets the column with the specified name.
        /// </summary>
        /// <returns>
        /// The column with the specified name as an <see cref="T:System.Object"/>.
        /// </returns>
        /// <param name="name">The name of the column to find. </param>
        /// <exception cref="T:System.IndexOutOfRangeException">No column with the specified name was found. </exception>
        /// <filterpriority>2</filterpriority>
        public override object this[string name] => CurrentRow.GetValue(_scheme.RequireField(name).Ordinal);

        /// <summary>
        /// Closes the <see cref="T:System.Data.IDataReader"/> Object.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public override void Close() => Dispose(true);

        /// <summary>
        /// Returns a <see cref="T:System.Data.DataTable"/> that describes the column metadata of the <see cref="T:System.Data.IDataReader"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Data.DataTable"/> that describes the column metadata.
        /// </returns>
        /// <exception cref="T:System.InvalidOperationException">The <see cref="T:System.Data.IDataReader"/> is closed. </exception>
        /// <filterpriority>2</filterpriority>
        public override DataTable GetSchemaTable()
        {
            var dataTable = new DataTable("SchemaTable") { Locale = CultureInfo.InvariantCulture, MinimumCapacity = _scheme.Fields.Count };
            var cColumnName = new DataColumn(SchemaTableColumn.ColumnName, typeof(string));
            var cColumnOrdinal = new DataColumn(SchemaTableColumn.ColumnOrdinal, typeof(int));
            var cColumnSize = new DataColumn(SchemaTableColumn.ColumnSize, typeof(int));
            var cNumericPrecision = new DataColumn(SchemaTableColumn.NumericPrecision, typeof(short));
            var cNumericScale = new DataColumn(SchemaTableColumn.NumericScale, typeof(short));
            var cDataType = new DataColumn(SchemaTableColumn.DataType, typeof(Type));
            var cProviderSpecificDataType = new DataColumn(SchemaTableOptionalColumn.ProviderSpecificDataType, typeof(Type));
            var cNonVersionedProviderType = new DataColumn(SchemaTableColumn.NonVersionedProviderType, typeof(int));
            var cProviderType = new DataColumn(SchemaTableColumn.ProviderType, typeof(int));
            var cIsLong = new DataColumn(SchemaTableColumn.IsLong, typeof(bool));
            var cAllowDbNull = new DataColumn(SchemaTableColumn.AllowDBNull, typeof(bool));
            var cIsReadOnly = new DataColumn(SchemaTableOptionalColumn.IsReadOnly, typeof(bool));
            var cIsRowVersion = new DataColumn(SchemaTableOptionalColumn.IsRowVersion, typeof(bool));
            var sIsUnique = new DataColumn(SchemaTableColumn.IsUnique, typeof(bool));
            var sIsKey = new DataColumn(SchemaTableColumn.IsKey, typeof(bool));
            var cIsAutoIncrement = new DataColumn(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool));
            var sIsHidden = new DataColumn(SchemaTableOptionalColumn.IsHidden, typeof(bool));
            var cBaseCatalogName = new DataColumn(SchemaTableOptionalColumn.BaseCatalogName, typeof(string));
            var cBaseSchemaName = new DataColumn(SchemaTableColumn.BaseSchemaName, typeof(string));
            var cBaseTableName = new DataColumn(SchemaTableColumn.BaseTableName, typeof(string));
            var cBaseColumnName = new DataColumn(SchemaTableColumn.BaseColumnName, typeof(string));
            var cBaseServerName = new DataColumn(SchemaTableOptionalColumn.BaseServerName, typeof(string));
            var cIsAliased = new DataColumn(SchemaTableColumn.IsAliased, typeof(bool));
            var sIsExpression = new DataColumn(SchemaTableColumn.IsExpression, typeof(bool));
            var cIsIdentity = new DataColumn("IsIdentity", typeof(bool));
            var cDataTypeName = new DataColumn("DataTypeName", typeof(string));
            var cUdtAssemblyQualifiedName = new DataColumn("UdtAssemblyQualifiedName", typeof(string));
            var cXmlSchemaCollectionDatabase = new DataColumn("XmlSchemaCollectionDatabase", typeof(string));
            var cXmlSchemaCollectionOwningSchema = new DataColumn("XmlSchemaCollectionOwningSchema", typeof(string));
            var cXmlSchemaCollectionName = new DataColumn("XmlSchemaCollectionName", typeof(string));
            var cIsColumnSet = new DataColumn("IsColumnSet", typeof(bool));

            cColumnOrdinal.DefaultValue = 0;
            cIsLong.DefaultValue = false;
            
            var columns = dataTable.Columns;
            columns.Add(cColumnName);
            columns.Add(cColumnOrdinal);
            columns.Add(cColumnSize);
            columns.Add(cNumericPrecision);
            columns.Add(cNumericScale);
            columns.Add(sIsUnique);
            columns.Add(sIsKey);
            columns.Add(cBaseServerName);
            columns.Add(cBaseCatalogName);
            columns.Add(cBaseColumnName);
            columns.Add(cBaseSchemaName);
            columns.Add(cBaseTableName);
            columns.Add(cDataType);
            columns.Add(cAllowDbNull);
            columns.Add(cProviderType);
            columns.Add(cIsAliased);
            columns.Add(sIsExpression);
            columns.Add(cIsIdentity);
            columns.Add(cIsAutoIncrement);
            columns.Add(cIsRowVersion);
            columns.Add(sIsHidden);
            columns.Add(cIsLong);
            columns.Add(cIsReadOnly);
            columns.Add(cProviderSpecificDataType);
            columns.Add(cDataTypeName);
            columns.Add(cXmlSchemaCollectionDatabase);
            columns.Add(cXmlSchemaCollectionOwningSchema);
            columns.Add(cXmlSchemaCollectionName);
            columns.Add(cUdtAssemblyQualifiedName);
            columns.Add(cNonVersionedProviderType);
            columns.Add(cIsColumnSet);
            
            foreach (var field in _scheme.Fields)
            {
                var systemType = RowData.DeriveSystemType(field.DataType);

                var row = dataTable.NewRow();
                row[cColumnName] = field.Name;
                row[cColumnOrdinal] = field.Ordinal;
                row[cColumnSize] = 0; 
                row[cDataType] = systemType;
                row[cProviderSpecificDataType] = systemType;
                row[cNonVersionedProviderType] = field.DataType;
                row[cDataTypeName] = GetDataTypeName(field.Ordinal);
                row[cProviderType] = field.DataType;
                row[cNumericPrecision] = DBNull.Value;
                row[cNumericScale] = DBNull.Value;
                row[cAllowDbNull] = true;
                row[cIsAliased] = false;
                row[sIsKey] = false;
                row[sIsHidden] = false;
                row[sIsExpression] = false;
                row[cIsIdentity] = false;
                row[cIsAutoIncrement] = false;
                row[cIsLong] = false;
                row[sIsUnique] = false;
                row[cIsRowVersion] = false;
                row[cIsReadOnly] = false;
                row[cIsColumnSet] = false;
                dataTable.Rows.Add(row);
                row.AcceptChanges();
            }
            
            foreach (DataColumn dataColumn in columns)
            {
                dataColumn.ReadOnly = true;
            }

            return dataTable;
        }
        /// <summary>
        /// Advances the data reader to the next result, when reading the results of batch SQL statements.
        /// </summary>
        /// <returns>
        /// true if there are more rows; otherwise, false.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool NextResult() => throw new NotSupportedException();

        /// <summary>
        /// Advances the <see cref="T:System.Data.IDataReader"/> to the next record.
        /// </summary>
        /// <returns>
        /// true if there are more rows; otherwise, false.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool Read()
        {
            if (_state != ReaderState.Closed)
            {
                try
                {
                    var haveItem = _protocolUtility.PqlCall.ResponseStream
                        .MoveNext(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                    if (haveItem)
                    {
                        _state = ReaderState.Fetching;
                        using var stream = new MemoryStream(_protocolUtility.PqlCall.ResponseStream.Current.Row.ToByteArray());
                        using var reader = new BinaryReader(stream);
                        _currentRow.Read(reader);
                    }
                    _state = haveItem ? _state = ReaderState.Fetching : ReaderState.Closed;
                }
                catch
                {
                    // mark connection as bad, because RowData.Read only throws in case of network error or data corruption
                    var conn = _protocolUtility.Connection;
                    if (conn != null)
                    {
                        conn.ConfirmExecutionCompletion(false);
                        _protocolUtility.Connection = null;
                    }

                    Close();
                    throw;
                }
            }

            return _state == ReaderState.Fetching;
        }

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        /// <returns>
        /// The level of nesting.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int Depth => 0;

        /// <summary>
        /// Gets a value indicating whether the data reader is closed.
        /// </summary>
        /// <returns>
        /// true if the data reader is closed; otherwise, false.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool IsClosed => _state == ReaderState.Closed || _protocolUtility.Connection == null;

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        /// <returns>
        /// The number of rows changed, inserted, or deleted; 0 if no rows were affected or the statement failed; and -1 for SELECT statements.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int RecordsAffected => throw new NotSupportedException();

        private RowData CurrentRow => _state == ReaderState.Fetching 
                    ? _currentRow 
                    : throw new InvalidOperationException("Don't have row data at this time");

        internal enum ReaderState
        {
            New,
            Fetching,
            Closed
        }
    }
}
