using System;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;

namespace Pql.ClientDriver.Wcf
{
    /// <summary>
    /// This custom message encoder helps to get rid of default XML/SOAP features and work with raw data streams over TCP/IP.
    /// Works with <see cref="PqlMessageEncodingElement"/> and <see cref="PqlMessageEncodingBindingElement"/>.
    /// Server interface must be based on Message in-out unimethod.
    /// Serialization, encoding and formatting features of WCF are completely disabled.
    /// To use, construct a custom binding with two binding elements: <see cref="TcpTransportBindingElement"/> and <see cref="PqlMessageEncodingBindingElement"/>.
    /// Transport must operate in streaming mode, buffering is not supported.
    /// </summary>
    public sealed class PqlMessageEncodingBindingElement 
                        : MessageEncodingBindingElement 
                        , IPolicyExportExtension
    {
        /// <summary>
        /// When overridden in a derived class, creates a factory for producing message encoders.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.ServiceModel.Channels.MessageEncoderFactory"/> used to produce message encoders.
        /// </returns>
        public override MessageEncoderFactory CreateMessageEncoderFactory()
        {
            return new PqlMessageEncoderFactory();
        }

        /// <summary>
        /// When overridden in a derived class, gets or sets the message version that can be handled by the message encoders produced by the message encoder factory.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.ServiceModel.Channels.MessageVersion"/> used by the encoders produced by the message encoder factory.
        /// </returns>
        public override MessageVersion MessageVersion
        {
            get { return MessageVersion.None; }
            set {  }
        }

        /// <summary>
        /// When overridden in a derived class, returns a copy of the binding element object.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.ServiceModel.Channels.BindingElement"/> object that is a deep clone of the original.
        /// </returns>
        public override BindingElement Clone()
        {
            return new PqlMessageEncodingBindingElement();
        }

        /// <summary>
        /// Initializes a channel factory for producing channels of a specified type from the binding context.
        /// </summary>
        /// <param name="context">The <see cref="T:System.ServiceModel.Channels.BindingContext"/> that provides context for the binding element. </param>
        /// <typeparam name="TChannel">The type of channel the factory builds.</typeparam
        /// ><exception cref="T:System.ArgumentNullException"><paramref name="context"/> is null.</exception>
        public override IChannelFactory<TChannel> BuildChannelFactory<TChannel>(BindingContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            context.BindingParameters.Add(this);
            return context.BuildInnerChannelFactory<TChannel>();
        }

        /// <summary>
        /// Initializes a channel listener to accept channels of a specified type from the binding context.
        /// </summary>
        /// <returns>
        /// The <see cref="T:System.ServiceModel.Channels.IChannelListener`1"/> of type <see cref="T:System.ServiceModel.Channels.IChannel"/> initialized from the <paramref name="context"/>.
        /// </returns>
        /// <param name="context">The <see cref="T:System.ServiceModel.Channels.BindingContext"/> that provides context for the binding element.</param>
        /// <typeparam name="TChannel">The type of channel the listener is built to accept.</typeparam>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="context"/> is null.</exception>
        public override IChannelListener<TChannel> BuildChannelListener<TChannel>(BindingContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            context.BindingParameters.Add(this);
            return context.BuildInnerChannelListener<TChannel>();
        }

        /// <summary>
        /// Returns a value that indicates whether the binding element can build a listener for a specific type of channel.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.ServiceModel.Channels.IChannelListener`1"/> of type <see cref="T:System.ServiceModel.Channels.IChannel"/> can be built by the binding element; otherwise, false.
        /// </returns>
        /// <param name="context">The <see cref="T:System.ServiceModel.Channels.BindingContext"/> that provides context for the binding element. </param>
        /// <typeparam name="TChannel">The type of channel the listener accepts.</typeparam>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="context"/> is null.</exception>
        public override bool CanBuildChannelListener<TChannel>(BindingContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            context.BindingParameters.Add(this);
            return context.CanBuildInnerChannelListener<TChannel>();
        }

        /// <summary>
        /// Implement to include for exporting a custom policy assertion about bindings.
        /// </summary>
        /// <param name="exporter">The <see cref="T:System.ServiceModel.Description.MetadataExporter"/> that you can use to modify the exporting process.</param>
        /// <param name="context">The <see cref="T:System.ServiceModel.Description.PolicyConversionContext"/> that you can use to insert your custom policy assertion.</param>
        void IPolicyExportExtension.ExportPolicy(MetadataExporter exporter, PolicyConversionContext context)
        {
        }
    }
}
