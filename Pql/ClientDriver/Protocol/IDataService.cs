using System.ServiceModel;
using System.ServiceModel.Channels;
using Pql.ClientDriver.Wcf;

namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// WCF service contract for PQL query processor.
    /// Works in conjunction with custom binding and encoder, <see cref="PqlMessageEncodingBindingElement"/>.
    /// Requires streaming mode for transport.
    /// </summary>
    [ServiceContract]
    public interface IDataService
    {
        /// <summary>
        /// Unimethod.
        /// </summary>
        /// <param name="request">Incoming message, must be of type <see cref="PqlMessage"/></param>
        /// <returns>Output message, of type <see cref="PqlMessage"/></returns>
        [OperationContract(Action = "*", ReplyAction = "*")]
        Message Process(Message request);
    }
}