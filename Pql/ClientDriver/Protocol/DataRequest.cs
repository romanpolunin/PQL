using ProtoBuf;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Input command.
    /// </summary>
    [ProtoContract]
    public class DataRequest
    {
        /// <summary>
        /// PQL query text. Optional, not used for bulk requests (see <see cref="HaveRequestBulk"/>).
        /// </summary>
        [ProtoMember(1, IsRequired = true)]
        public string CommandText;

        /// <summary>
        /// True to indicate that client wants server to validate the query and cache execution plan.
        /// If true, server will not perform any actual data access beside query compilation and validation.
        /// </summary>
        [ProtoMember(2, IsRequired = true)]
        public bool PrepareOnly;

        /// <summary>
        /// True to indicate that client is expecting to receive a dataset in response.
        /// If false, server will only stream the general status response back.
        /// </summary>
        [ProtoMember(3, IsRequired = true)]
        public bool ReturnDataset;

        /// <summary>
        /// True to indicate that client has supplied an instance of <see cref="DataRequestParams"/> in request stream.
        /// </summary>
        [ProtoMember(4, IsRequired = true)]
        public bool HaveParameters;
        
        /// <summary>
        /// True to indicate that client has supplied an instance of <see cref="DataRequestBulk"/> in request stream.
        /// </summary>
        [ProtoMember(5, IsRequired = true)]
        public bool HaveRequestBulk;
        
        /// <summary>
        /// Resets content of this request.
        /// Used for pooling.
        /// </summary>
        public void Clear()
        {
            CommandText = null;
            PrepareOnly = false;
            ReturnDataset = false;
            HaveParameters = false;
            HaveRequestBulk = false;
        }
    }
}