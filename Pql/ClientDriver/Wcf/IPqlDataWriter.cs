using System;
using System.IO;

namespace Pql.ClientDriver.Wcf
{
    /// <summary>
    /// Producer's callback interface. 
    /// Used by <see cref="PqlMessageEncodingBindingElement"/>'s custom encoder to write data into network output stream.
    /// </summary>
    public interface IPqlDataWriter : IDisposable
    {
        /// <summary>
        /// Writes data to output stream.
        /// </summary>
        void WriteTo(Stream stream);
    }
}