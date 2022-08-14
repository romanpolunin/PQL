using System.Text.Json.Serialization;

using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.Interfaces.Services
{
    public struct DataContainerStatsFile
    {
        [JsonInclude]
        public string DriverVersion { get; }
        [JsonInclude]
        public DataContainerStats DataContainerStats { get; }

        public DataContainerStatsFile(string version, DataContainerStats stats)
        {
            DriverVersion = string.IsNullOrEmpty(version) ? throw new ArgumentNullException(nameof(version)) : version.ToString();
            DataContainerStats = stats ?? throw new ArgumentNullException(nameof(stats));
        }
    }
}