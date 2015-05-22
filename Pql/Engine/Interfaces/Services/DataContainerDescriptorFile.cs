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
                throw new ArgumentNullException("version");
            }

            if (descriptor == null)
            {
                throw new ArgumentNullException("descriptor");
            }

            DriverVersion = version.ToString();
            DataContainerDescriptor = descriptor;
        }
    }
}