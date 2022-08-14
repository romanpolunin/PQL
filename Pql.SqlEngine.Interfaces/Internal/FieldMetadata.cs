using System.Data;
using System.Text.Json.Serialization;

namespace Pql.SqlEngine.Interfaces.Internal
{
    /// <summary>
    /// Logical definition of a single attribute of some entity.
    /// </summary>
    public sealed class FieldMetadata
    {
        /// <summary>
        /// Internal identifier of the field.
        /// </summary>
        [JsonInclude]
        public int FieldId;

        /// <summary>
        /// Name.
        /// </summary>
        [JsonInclude]
        public string Name;
        
        /// <summary>
        /// Label.
        /// </summary>
        [JsonInclude]
        public string DisplayName;
        
        /// <summary>
        /// Data type.
        /// </summary>
        [JsonInclude]
        public DbType DbType;

        /// <summary>
        /// Reference to owner entity.
        /// </summary>
        [JsonInclude]
        public int OwnerDocumentType;

        /// <summary>
        /// Full name (not assembly-qualified, namespace only) type name that is supposed to be serialized into this binary field.
        /// Only meaningful with fields of type DbType.Object or DbType.Binary.
        /// </summary>
        [JsonInclude]
        public string? SerializationTypeName;

        /// <summary>
        /// Dynamically determined actual serialization type. See <see cref="SerializationTypeName"/>.
        /// </summary>
        [JsonIgnore]
        public Type? SerializationType;

        private FieldMetadata()
        {
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public FieldMetadata(int id, string name, string displayName, DbType dbType, int ownerDocType)
        {
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(id), "FieldId must be positive");
            }

            if (ownerDocType <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(ownerDocType), "ownerDocType must be positive");
            }

            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (string.IsNullOrEmpty(displayName))
            {
                throw new ArgumentNullException(nameof(displayName));
            }

            if (!Enum.IsDefined(typeof (DbType), dbType))
            {
                throw new ArgumentOutOfRangeException(nameof(dbType), "DbType is out of range: " + (int)dbType);
            }

            FieldId = id;
            Name = name;
            DisplayName = displayName;
            DbType = dbType;
            OwnerDocumentType = ownerDocType;
        }
    }
}