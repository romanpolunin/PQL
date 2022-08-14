using System.Text.Json.Serialization;

using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.Interfaces.Services
{
    public struct DataContainerDescriptorFile
    {
        [JsonInclude]
        public readonly string DriverVersion;
        [JsonInclude]
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