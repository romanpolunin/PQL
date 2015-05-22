using System.Data;
using ProtoBuf;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Headers for parameters part of the request.
    /// Number of elements in <see cref="Names"/> and other arrays must match.
    /// </summary>
    [ProtoContract]
    public class DataRequestParams
    {
        /// <summary>
        /// Names of parameters.
        /// Number of elements in <see cref="Names"/> and other arrays must match.
        /// </summary>
        [ProtoMember(1, IsRequired = true)]
        public string[] Names;

        /// <summary>
        /// Data types of parameters. For parameters with collections, indicates individual element's type.
        /// Number of elements in <see cref="Names"/> and other arrays must match.
        /// </summary>
        [ProtoMember(2, IsRequired = true)]
        public DbType[] DataTypes;

        /// <summary>
        /// A BitVector. For every parameter, corresponding bit indicates whether parameter comes with a collection of values.
        /// Number of elements in <see cref="Names"/> and other arrays must match. 
        /// </summary>
        [ProtoMember(3, IsRequired = true)]
        public int[] IsCollectionFlags;

        /// <summary>
        /// Parameter values.
        /// </summary>
        public PqlDataCommandParameterCollection Bulk;

        /// <summary>
        /// Resets content of this request.
        /// Used for pooling.
        /// </summary>
        public void Clear()
        {
            Names = null;
            DataTypes = null;
            IsCollectionFlags = null;
        }
    }
}