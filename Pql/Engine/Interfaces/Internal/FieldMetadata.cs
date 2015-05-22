using System;
using System.Data;
using System.Runtime.Serialization;

namespace Pql.Engine.Interfaces.Internal
{
    /// <summary>
    /// Logical definition of a single attribute of some entity.
    /// </summary>
    [DataContract]
    public sealed class FieldMetadata
    {
        /// <summary>
        /// Internal identifier of the field.
        /// </summary>
        [DataMember]
        public int FieldId;

        /// <summary>
        /// Name.
        /// </summary>
        [DataMember]
        public string Name;
        
        /// <summary>
        /// Label.
        /// </summary>
        [DataMember]
        public string DisplayName;
        
        /// <summary>
        /// Data type.
        /// </summary>
        [DataMember]
        public DbType DbType;

        /// <summary>
        /// Reference to owner entity.
        /// </summary>
        [DataMember]
        public int OwnerDocumentType;

        /// <summary>
        /// Full name (not assembly-qualified, namespace only) type name that is supposed to be serialized into this binary field.
        /// Only meaningful with fields of type DbType.Object or DbType.Binary.
        /// </summary>
        [DataMember]
        public string SerializationTypeName;

        /// <summary>
        /// Dynamically determined actual serialization type. See <see cref="SerializationTypeName"/>.
        /// </summary>
        [IgnoreDataMember]
        public Type SerializationType;

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
                throw new ArgumentOutOfRangeException("id", "FieldId must be positive");
            }

            if (ownerDocType <= 0)
            {
                throw new ArgumentOutOfRangeException("ownerDocType", "ownerDocType must be positive");
            }

            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            if (string.IsNullOrEmpty(displayName))
            {
                throw new ArgumentNullException("displayName");
            }

            if (!Enum.IsDefined(typeof (DbType), dbType))
            {
                throw new ArgumentOutOfRangeException("dbType", "DbType is out of range: " + (int)dbType);
            }

            FieldId = id;
            Name = name;
            DisplayName = displayName;
            DbType = dbType;
            OwnerDocumentType = ownerDocType;
        }
    }
}