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
                throw new ArgumentNullException("version");
            }

            if (stats == null)
            {
                throw new ArgumentNullException("stats");
            }

            DriverVersion = version.ToString();
            DataContainerStats = stats;
        }
    }
}