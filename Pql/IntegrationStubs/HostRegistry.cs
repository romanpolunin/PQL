using System;
using System.Configuration;
using System.Linq;
using StructureMap.Configuration.DSL;
using StructureMap.Graph;
using StructureMap.TypeRules;

namespace Pql.IntegrationStubs
{
    public class HostRegistry : Registry
    {
        public class SingletonConvention<TPluginFamily> : IRegistrationConvention
        {
            public void Process(Type type, Registry registry)
            {
                if (!type.IsConcrete() || !type.CanBeCreated() || !type.AllInterfaces().Contains(typeof(TPluginFamily)))
                {
                    return;
                }

                registry.For(typeof(TPluginFamily)).Singleton().Use(type);
            }
        }

        public HostRegistry()
        {
            Scan(scanner =>
                {
                    For<IHostedProcessArgs>().Singleton().Use<HostedProcessArgs>();
                    For<WindowsServiceConfigSection>().Transient().Use(
                        () =>
                            (WindowsServiceConfigSection) ConfigurationManager.GetSection("thinksmartHost") 
                            ?? new WindowsServiceConfigSection()
                        );
                    AddType(typeof(WindowsHostService), typeof(WindowsHostService));
                });
        }
    }
}