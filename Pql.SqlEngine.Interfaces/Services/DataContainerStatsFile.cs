using System;
using System.Runtime.Serialization;
using Pql.Engine.Interfaces.Internal;

namespace Pql.Engine.Interfaces.Services
{
    [DataContract]
    public struct DataContainerStatsFile
    {
        [DataMember]
        public readonly string DriverVersion;
        [DataMember]
        public readonly DataContainerStats DataContainerStats;

        public DataContainerStatsFile(Version version, DataContainerStats stats)
        {
            if (version == null)
            {
                throw new ArgumentNullException(nameof(version));
            }

            DriverVersion = version.ToString();
            DataContainerStats = stats ?? throw new ArgumentNullException(nameof(stats));
        }
    }
}