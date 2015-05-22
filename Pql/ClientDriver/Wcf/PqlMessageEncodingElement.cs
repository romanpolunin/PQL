using System;
using System.Configuration;
using System.ServiceModel.Channels;
using System.ServiceModel.Configuration;

namespace Pql.ClientDriver.Wcf
{
    /// <summary>
    /// This is a configuration element for <see cref="PqlMessageEncodingBindingElement"/>.
    /// </summary>
    public class PqlMessageEncodingElement : BindingElementExtensionElement
    {
        /// <summary>
        /// When overridden in a derived class, gets the <see cref="T:System.Type"/> object that represents the custom binding element. 
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Type"/> object that represents the custom binding type.
        /// </returns>
        public override Type BindingElementType
        {
            get { return typeof(PqlMessageEncodingBindingElement); }
        }

        /// <summary>
        /// Compression level to use. Currently ignored.
        /// </summary>
        [ConfigurationProperty("compressionLevel", DefaultValue = "NoCompression")]
        public string InnerMessageEncoding
        {
            get { return (string)base["compressionLevel"]; }
            set { base["compressionLevel"] = value; }
        }

        /// <summary>
        /// Applies the content of a specified binding element to this binding configuration element.
        /// </summary>
        /// <param name="bindingElement">A binding element.</param><exception cref="T:System.ArgumentNullException"><paramref name="bindingElement"/> is null.</exception>
        public override void ApplyConfiguration(BindingElement bindingElement)
        {
        }

        /// <summary>
        /// When overridden in a derived class, returns a custom binding element object. 
        /// </summary>
        /// <returns>
        /// A custom <see cref="T:System.ServiceModel.Channels.BindingElement"/> object.
        /// </returns>
        protected override BindingElement CreateBindingElement()
        {
            var bindingElement = new PqlMessageEncodingBindingElement();
            this.ApplyConfiguration(bindingElement);
            return bindingElement;
        }
    }
}