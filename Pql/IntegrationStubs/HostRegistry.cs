using System;
using System.Configuration;
using System.Linq;
using StructureMap;
using StructureMap.Configuration.DSL;
using StructureMap.Graph;
using StructureMap.Graph.Scanning;
using StructureMap.TypeRules;

namespace Pql.IntegrationStubs
{
    public class HostRegistry : Registry
    {
        public class SingletonConvention<TPluginFamily> : IRegistrationConvention
        {
            public void ScanTypes(TypeSet types, Registry registry)
            {
                foreach (var type in types.AllTypes())
                {
                    if (!type.IsConcrete() || !type.CanBeCreated() || !type.AllInterfaces().Contains(typeof(TPluginFamily)))
                    {
                        return;
                    }

                    registry.For(typeof(TPluginFamily)).Singleton().Use(type);
                }                
            }
        }

        public HostRegistry()
        {
            Scan(scanner =>
                {
                    For<IHostedProcessArgs>().Singleton().Use<HostedProcessArgs>();
                    For<WindowsServiceConfigSection>().Transient().Use(
                        () =>
                            (WindowsServiceConfigSection) ConfigurationManager.GetSection("mycompanyHost") 
                            ?? new WindowsServiceConfigSection()
                        );
                    AddType(typeof(WindowsHostService), typeof(WindowsHostService));
                    scanner.WithDefaultConventions();
                });
        }
    }
}