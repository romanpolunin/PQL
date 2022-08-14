using System.Data;

using Google.Protobuf.Collections;

using Pql.SqlEngine.Interfaces;

namespace Pql.Server.Protocol.Wire
{
    public partial class DataResponseField
    {
        public DbType DataType { get => (DbType)DataTypeWire; set { DataTypeWire = (int)value; } }
    }

    public partial class DataRequest
    {
        public DateTime Created
        {
            get => CreatedWire.ToDateTime();
            set => CreatedWire = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow);
        }
    }

    public partial class DataRequestParams 
    {
        public PqlDataCommandParameterCollection Bulk { get; set; }
    }

    public partial class DataRequestBulk
    {
        public StatementType DbStatementType
        {
            get => (StatementType)DbStatementTypeWire;
            set => DbStatementTypeWire = (int)value;
        }
        public IEnumerable<RowData> Bulk { get; set; }
    }

    public struct DbTypeIndexer
    {
        private readonly RepeatedField<int> _items;

        public DbTypeIndexer(RepeatedField<int> items)
        {
            _items = items;
        }

        public DbType this[int index]
        {
            get => (DbType)_items[index];
            set => _items[index] = (int)value;
        }
    }

    public struct BitVectorWrapper
    {
        private readonly RepeatedField<int> _items;

        public BitVectorWrapper(RepeatedField<int> items)
        {
            _items = items;
        }

        public bool this[int index]
        {
            get => BitVector.Get(_items, index);
            set
            {
                if (value)
                {
                    BitVector.Set(_items, index);
                }
                else
                {
                    BitVector.Clear(_items, index);
                }
            }
        }
    }

    /// <summary>
    /// Data server's response schema. Gets written into the stream, then comes data stream.
    /// </summary>
    public partial class DataResponse
    {
        /// <summary>
        /// Ctr.
        /// </summary>
        public DataResponse(int errorCode, string serverMessage) : this()
        {
            if (errorCode != 0 && string.IsNullOrEmpty(serverMessage))
            {
                throw new ArgumentNullException(serverMessage);
            }

            ErrorCode = errorCode;
            ServerMessage = serverMessage;
        }

        /// <summary>
        /// Looks up a field object by its internal name.
        /// </summary>
        /// <param name="name">Name to look for</param>
        /// <returns>Field metadata object</returns>
        /// <exception cref="IndexOutOfRangeException">Unknown name</exception>
        /// <exception cref="ArgumentNullException">Name is null</exception>
        public DataResponseField RequireField(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (!_fieldsByName.TryGetValue(name, out _))
            {
                throw new IndexOutOfRangeException("Unknown field: " + name);
            }

            return _fieldsByName[name];
        }

        /// <summary>
        /// Looks up a field object by its index in the fields metadata.
        /// </summary>
        /// <param name="indexInResponse">Index of the field in the metadata</param>
        /// <returns>Field metadata object</returns>
        /// <exception cref="IndexOutOfRangeException">Invalid field index</exception>
        public DataResponseField RequireField(int indexInResponse)
        {
            if (indexInResponse < 0 || indexInResponse >= Fields.Count)
            {
                throw new IndexOutOfRangeException("Invalid field index: " + indexInResponse);
            }

            return Fields[indexInResponse];
        }

        /// <summary>
        /// Looks up a field name by its index in the fields metadata.
        /// </summary>
        /// <param name="indexInResponse">Index of the field in the metadata</param>
        /// <returns>Internal name of the field</returns>
        public string GetName(int indexInResponse) => RequireField(indexInResponse).Name;

        /// <summary>
        /// Reassembles the field-by-name lookup dictionary after deserialization.
        /// </summary>
        partial void OnConstruction()
        {
            if (Fields == null && string.IsNullOrEmpty(ServerMessage))
            {
                throw new InvalidOperationException("Fields or error message must be initialized");
            }

            if (Fields != null)
            {
                _fieldsByName = new Dictionary<string, DataResponseField>();
                var ordinal = 0;
                foreach (var field in Fields)
                {
                    if (field == null)
                    {
                        throw new InvalidOperationException("Field is null as position " + ordinal);
                    }

                    if (ordinal != field.Ordinal)
                    {
                        throw new InvalidOperationException(string.Format("Field {0} is listed out of order. Found at {1}, expected at {2}", field.Name, ordinal, field.Ordinal));
                    }

                    while (_fieldsByName.ContainsKey(field.Name))
                    {
                        field.Name = string.Format("{0}({1})", field.Name, ordinal);
                    }

                    _fieldsByName.Add(field.Name, field);
                    ordinal++;
                }
            }
        }

        private Dictionary<string, DataResponseField> _fieldsByName;
    }
}