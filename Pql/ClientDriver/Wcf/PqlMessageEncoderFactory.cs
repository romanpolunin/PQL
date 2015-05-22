using System;
using System.ServiceModel.Channels;

namespace Pql.ClientDriver.Wcf
{
    /// <summary>
    /// This custom message encoder helps to get rid of default XML/SOAP features and work with raw data streams over TCP/IP.
    /// Works with <see cref="PqlMessageEncodingElement"/> and <see cref="PqlMessageEncodingBindingElement"/>.
    /// Server interface must be based on Message in-out unimethod.
    /// Serialization, encoding and formatting features of WCF are completely disabled.
    /// To use, construct a custom binding with two binding elements: <see cref="TcpTransportBindingElement"/> and <see cref="PqlMessageEncodingBindingElement"/>.
    /// Transport must operate in streaming mode, buffering is not supported.
    /// Custom message encoder expects all messages to be of type <see cref="PqlMessage"/>.
    /// </summary>
    internal class PqlMessageEncoderFactory : MessageEncoderFactory
    {
        readonly MessageEncoder m_encoder;

        /// <summary>
        /// Ctr.
        /// </summary>
        public PqlMessageEncoderFactory()
        {
            m_encoder = new PqlMessageEncoder();
        }

        /// <summary>
        /// When overridden in a derived class, gets the message encoder that is produced by the factory.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.ServiceModel.Channels.MessageEncoder"/> used by the factory.
        /// </returns>
        public override MessageEncoder Encoder
        {
            get { return m_encoder; }
        }

        /// <summary>
        /// When overridden in a derived class, gets the message version that is used by the encoders produced by the factory to encode messages.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.ServiceModel.Channels.MessageVersion"/> used by the factory.
        /// </returns>
        public override MessageVersion MessageVersion
        {
            get { return m_encoder.MessageVersion; }
        }

        /// <summary>
        /// Actual encoder.
        /// </summary>
        class PqlMessageEncoder : MessageEncoder
        {
            private const string PqlContentType = "application/x-pql";

            /// <summary>
            /// When overridden in a derived class, gets the MIME content type used by the encoder.
            /// </summary>
            /// <returns>
            /// The content type that is supported by the message encoder.
            /// </returns>
            public override string ContentType
            {
                get { return PqlContentType; }
            }

            /// <summary>
            /// When overridden in a derived class, gets the media type value that is used by the encoder.
            /// </summary>
            /// <returns>
            /// The media type that is supported by the message encoder.
            /// </returns>
            public override string MediaType
            {
                get { return PqlContentType; }
            }

            /// <summary>
            /// When overridden in a derived class, gets the message version value that is used by the encoder.
            /// </summary>
            /// <returns>
            /// The <see cref="T:System.ServiceModel.Channels.MessageVersion"/> that is used by the encoder.
            /// </returns>
            public override MessageVersion MessageVersion
            {
                get { return MessageVersion.None; }
            }

            /// <summary>
            /// When overridden in a derived class, reads a message from a specified stream.
            /// </summary>
            /// <returns>
            /// The <see cref="T:System.ServiceModel.Channels.Message"/> that is read from the stream specified.
            /// </returns>
            /// <param name="buffer">A <see cref="T:System.ArraySegment`1"/> of type <see cref="T:System.Byte"/> that provides the buffer from which the message is deserialized.</param><param name="bufferManager">The <see cref="T:System.ServiceModel.Channels.BufferManager"/> that manages the buffer from which the message is deserialized.</param><param name="contentType">The Multipurpose Internet Mail Extensions (MIME) message-level content-type.</param>
            public override Message ReadMessage(ArraySegment<byte> buffer, BufferManager bufferManager, string contentType)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// When overridden in a derived class, writes a message of less than a specified size to a byte array buffer at the specified offset.
            /// </summary>
            /// <returns>
            /// A <see cref="T:System.ArraySegment`1"/> of type byte that provides the buffer to which the message is serialized.
            /// </returns>
            /// <param name="message">The <see cref="T:System.ServiceModel.Channels.Message"/> to write to the message buffer.</param><param name="maxMessageSize">The maximum message size that can be written.</param><param name="bufferManager">The <see cref="T:System.ServiceModel.Channels.BufferManager"/> that manages the buffer to which the message is written.</param><param name="messageOffset">The offset of the segment that begins from the start of the byte array that provides the buffer.</param>
            public override ArraySegment<byte> WriteMessage(Message message, int maxMessageSize, BufferManager bufferManager, int messageOffset)
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// When overridden in a derived class, reads a message from a specified stream.
            /// </summary>
            /// <returns>
            /// The <see cref="T:System.ServiceModel.Channels.Message"/> that is read from the stream specified.
            /// </returns>
            /// <param name="stream">The <see cref="T:System.IO.Stream"/> object from which the message is read.</param><param name="maxSizeOfHeaders">The maximum size of the headers that can be read from the message.</param><param name="contentType">The Multipurpose Internet Mail Extensions (MIME) message-level content-type.</param>
            public override Message ReadMessage(System.IO.Stream stream, int maxSizeOfHeaders, string contentType)
            {
                var message = new PqlMessage(stream, null);
                message.ReadHeaders();
                return message;
            }

            /// <summary>
            /// When overridden in a derived class, writes a message to a specified stream.
            /// </summary>
            /// <param name="message">The <see cref="T:System.ServiceModel.Channels.Message"/> to write to the <paramref name="stream"/>.</param><param name="stream">The <see cref="T:System.IO.Stream"/> object to which the <paramref name="message"/> is written.</param>
            public override void WriteMessage(Message message, System.IO.Stream stream)
            {
                if (message == null)
                {
                    throw new ArgumentNullException("message");
                }

                var pqlMessage = message as PqlMessage;
                if (pqlMessage == null)
                {
                    throw new Exception("This encoder only supports messages of type " + typeof(PqlMessage).AssemblyQualifiedName);
                }
                
                try
                {
                    pqlMessage.WriteHeaders(stream);

                    if (pqlMessage.Stream != null)
                    {
                        pqlMessage.Stream.CopyTo(stream);
                    }
                    else
                    {
                        pqlMessage.DataWriter.WriteTo(stream);
                    }
                }
                finally
                {
                    pqlMessage.Close();
                }
            }
        }
    }
}
