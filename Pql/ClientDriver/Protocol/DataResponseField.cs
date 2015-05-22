using System.Data;
using ProtoBuf;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Field metadata object. Holds information about a single column in the response dataset.
    /// </summary>
    [ProtoContract]
    public class DataResponseField
    {
        /// <summary>
        /// Internal name of the field.
        /// </summary>
        [ProtoMember(1, IsRequired = true)]
        public string Name;
        /// <summary>
        /// Display name of the field.
        /// </summary>
        [ProtoMember(2, IsRequired = true)]
        public string DisplayName;
        /// <summary>
        /// DbType of the field.
        /// </summary>
        [ProtoMember(3, IsRequired = true)]
        public DbType DataType;
        /// <summary>
        /// Index of this field in the response dataset.
        /// </summary>
        [ProtoMember(4, IsRequired = true)]
        public int Ordinal;
    }
}