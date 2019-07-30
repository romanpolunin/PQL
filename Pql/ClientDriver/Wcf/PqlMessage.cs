using System;
using System.IO;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading;
using System.Xml;

namespace Pql.ClientDriver.Wcf
{
    /// <summary>
    /// This custom message type helps to get rid of default XML/SOAP features and work with raw data streams over TCP/IP.
    /// Works with <see cref="PqlMessageEncodingElement"/> and <see cref="PqlMessageEncodingBindingElement"/>.
    /// Server interface must be based on Message in-out unimethod.
    /// Serialization, encoding and formatting features of WCF are completely disabled.
    /// To use, construct a custom binding with two binding elements: <see cref="TcpTransportBindingElement"/> and <see cref="PqlMessageEncodingBindingElement"/>.
    /// Transport must operate in streaming mode, buffering is not supported.
    /// </summary>
    public class PqlMessage : Message
    {
        private Stream m_stream;
        private readonly MessageHeaders m_headers;
        private readonly MessageProperties m_properties;
        private IPqlDataWriter m_dataWriter;
        private IDisposable[] m_holders;

        /// <summary>
        /// Ctr. Used on producer's side, which implements synchronous or asynchronous data generation process, 
        /// to buffer and pipeline the data straight into network output stream.
        /// </summary>
        /// <param name="dataWriter">Producer's callback interface</param>
        /// <param name="holders">Optional IDisposable to enable controlled cleanup of <paramref name="dataWriter"/> and others</param>
        /// <param name="authTicket">Authentication information, including tenantId, userId etc.</param>
        /// <param name="scopeId">Scope identifier or some other key</param>
        /// <param name="protocolVersion">Version information on the sender of this message</param>
        public PqlMessage(IPqlDataWriter dataWriter, IDisposable[] holders, string authTicket, string scopeId, string protocolVersion)
            : this()
        {
            if (string.IsNullOrWhiteSpace(authTicket))
            {
                throw new ArgumentNullException("authTicket");
            }

            if (string.IsNullOrWhiteSpace(scopeId))
            {
                throw new ArgumentNullException("scopeId");
            }

            if (string.IsNullOrWhiteSpace(protocolVersion))
            {
                throw new ArgumentNullException("protocolVersion");
            }

            AuthTicket = authTicket;
            ScopeId = scopeId;
            ProtocolVersion = protocolVersion;

            m_dataWriter = dataWriter ?? throw new ArgumentNullException("dataWriter");
            m_holders = holders;
        }

        /// <summary>
        /// Ctr. Used on consumer's side, to connect the raw network input stream.
        /// Client is completely responsible for the serialization and data format.
        /// </summary>
        /// <param name="stream">Content stream</param>
        /// <param name="holders">Optional IDisposable to enable controlled cleanup of <paramref name="stream"/></param>
        public PqlMessage(Stream stream, IDisposable[] holders) : this()
        {
            m_stream = stream;
            m_holders = holders;
        }

        /// <summary>
        /// Dummy contsructor, for testing only.
        /// </summary>
        internal PqlMessage()
        {
            m_properties = new MessageProperties();
            m_headers = new MessageHeaders(MessageVersion.None);
            CreatedOn = DateTime.Now;
        }

        /// <summary>
        /// Tenant Id. At this time, must be tenantId converted to string.
        /// </summary>
        public string AuthTicket { get; set; }
        /// <summary>
        /// Scope Id. At this time, must be workspace session Id converted to string.
        /// </summary>
        public string ScopeId { get; set; }
        /// <summary>
        /// Protocol version information.
        /// </summary>
        public string ProtocolVersion { get; set; }

        /// <summary>
        /// Local time when this message object was created.
        /// </summary>
        public DateTime CreatedOn { get; private set; }

        /// <summary>
        /// Reads message headers from stream.
        /// </summary>
        public void ReadHeaders()
        {
            if (ReferenceEquals(m_stream, null))
            {
                throw new InvalidOperationException("Stream is not set");
            }

            using (var reader = new StreamReader(m_stream, Encoding.UTF8, false, 100, true))
            {
                ProtocolVersion = reader.ReadLine();
                AuthTicket = reader.ReadLine();
                ScopeId = reader.ReadLine();
            }
        }

        /// <summary>
        /// Writes message headers to stream.
        /// </summary>
        public void WriteHeaders(Stream stream)
        {
            using (var writer = new StreamWriter(stream, Encoding.UTF8, 100, true))
            {
                writer.WriteLine(ProtocolVersion);
                writer.WriteLine(AuthTicket);
                writer.WriteLine(ScopeId);
                writer.Flush();
            }
        }

        /// <summary>
        /// The incoming (network) data stream. Usually assigned on consumer's side. 
        /// Either <see cref="Stream"/> or <see cref="DataWriter"/> must be initialized.
        /// </summary>
        public Stream Stream { get { return m_stream; } }
        
        /// <summary>
        /// The producer's callback interface. Usually assigned on producer's side.
        /// Either <see cref="Stream"/> or <see cref="DataWriter"/> must be initialized.
        /// </summary>
        public IPqlDataWriter DataWriter { get { return m_dataWriter; } }

        /// <summary>
        /// Called when the message body is written to an XML file.
        /// </summary>
        /// <param name="writer">A <see cref="T:System.Xml.XmlDictionaryWriter"/> that is used to write this message body to an XML file.</param>
        protected override void OnWriteBodyContents(XmlDictionaryWriter writer)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// When overridden in a derived class, gets the headers of the message. 
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.ServiceModel.Channels.MessageHeaders"/> object that represents the headers of the message. 
        /// </returns>
        /// <exception cref="T:System.ObjectDisposedException">The message has been disposed of.</exception>
        public override MessageHeaders Headers
        {
            get { return m_headers; }
        }

        /// <summary>
        /// When overridden in a derived class, gets a set of processing-level annotations to the message. 
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.ServiceModel.Channels.MessageProperties"/> that contains a set of processing-level annotations to the message.
        /// </returns>
        /// <exception cref="T:System.ObjectDisposedException">The message has been disposed of.</exception>
        public override MessageProperties Properties
        {
            get { return m_properties; }
        }

        /// <summary>
        /// When overridden in a derived class, gets the SOAP version of the message.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.ServiceModel.Channels.MessageVersion"/> object that represents the SOAP version. 
        /// </returns>
        /// <exception cref="T:System.ObjectDisposedException">The message has been disposed of.</exception>
        public override MessageVersion Version
        {
            get { return MessageVersion.None; }
        }

        /// <summary>
        /// Called when the message is closing (no need to implement Dispose here).
        /// </summary>
        protected override void OnClose()
        {
            var stream = Interlocked.CompareExchange(ref m_stream, null, m_stream);
            var holders = Interlocked.CompareExchange(ref m_holders, null, m_holders);
            Interlocked.CompareExchange(ref m_dataWriter, null, m_dataWriter);

            if (holders != null)
            {
                foreach (var holder in holders)
                {
                    if (holder != null)
                    {
                        holder.Dispose();
                    }
                }
            }
            
            if (stream != null)
            {
                stream.Close();
            }

            base.OnClose();
        }

        /// <summary>
        /// Called when a message buffer is created to store this message.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.ServiceModel.Channels.MessageBuffer"/> object for the newly created message copy.
        /// </returns>
        /// <param name="maxBufferSize">The maximum size of the buffer to be created.</param>
        protected override MessageBuffer OnCreateBufferedCopy(int maxBufferSize)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Called when an XML dictionary reader that accesses the body content of this message is retrieved.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Xml.XmlDictionaryReader"/> object that accesses the body content of this message.
        /// </returns>
        protected override XmlDictionaryReader OnGetReaderAtBodyContents()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets a value that indicates whether this message generates any SOAP faults.
        /// </summary>
        /// <returns>
        /// true if this message generates any SOAP faults; otherwise, false.
        /// </returns>
        /// <exception cref="T:System.ObjectDisposedException">The message has been disposed of.</exception>
        public override bool IsFault
        {
            get { return false; }
        }
    }
}