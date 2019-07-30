using Pql.Engine.DataContainer.RamDriver;
using Pql.Engine.Interfaces.Services;
using Pql.IntegrationStubs;
using StructureMap;

namespace Pql.Server
{
    public class HostedProcessRegistry : Registry
    {
        /// <summary>
        /// Registers hosted process for IoC
        /// </summary>
        public HostedProcessRegistry() 
        {
            AddType(typeof(IHostedProcess), typeof(DataServerProcess));
            AddType(typeof(IStorageDriverFactory), typeof(RamDriverFactory), "ram");
        }
    }
}