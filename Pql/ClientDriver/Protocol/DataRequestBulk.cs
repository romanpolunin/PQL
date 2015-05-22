using System.Collections.Generic;
using System.Data;
using ProtoBuf;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Headers for bulk part of the request.
    /// </summary>
    [ProtoContract]
    public class DataRequestBulk
    {
        /// <summary>
        /// Type of this bulk command.
        /// </summary>
        [ProtoMember(1, IsRequired = true)]
        public StatementType DbStatementType;

        /// <summary>
        /// Name of the entity that we select, insert, update or delete.
        /// </summary>
        [ProtoMember(2, IsRequired = true)]
        public string EntityName;

        /// <summary>
        /// Identifiers of fields whose values will be inserted.
        /// </summary>
        [ProtoMember(3, IsRequired = true)]
        public string[] FieldNames;

        /// <summary>
        /// Number of items that are going to be sent as part of this command.
        /// Helps processor to pre-allocate capacity for large INSERTs.
        /// Is not used for other statement types.
        /// </summary>
        [ProtoMember(4, IsRequired = true)]
        public int InputItemsCount;

        /// <summary>
        /// Data enumerator.
        /// </summary>
        public IEnumerable<RowData> Bulk;

        /// <summary>
        /// Resets content of this request.
        /// Used for pooling.
        /// </summary>
        public void Clear()
        {
            DbStatementType = 0;
            EntityName = null;
            FieldNames = null;
            InputItemsCount = 0;
        }
    }
}