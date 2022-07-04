using System;
using System.Runtime.Serialization;
using Pql.Engine.Interfaces.Internal;

namespace Pql.Engine.Interfaces.Services
{
    [DataContract]
    public struct DataContainerDescriptorFile
    {
        [DataMember]
        public readonly string DriverVersion;
        [DataMember]
        public readonly DataContainerDescriptor DataContainerDescriptor;

        public DataContainerDescriptorFile(Version version, DataContainerDescriptor descriptor)
        {
            if (version == null)
            {
                throw new ArgumentNullException(nameof(version));
            }

            DriverVersion = version.ToString();
            DataContainerDescriptor = descriptor ?? throw new ArgumentNullException(nameof(descriptor));
        }
    }
}