using System;
using System.Configuration;
using System.Configuration.Install;
using System.Reflection;

namespace Pql.IntegrationStubs
{
    public partial class ServiceInstallerBase : Installer
    {
        public ServiceInstallerBase()
        {
            InitializeComponent();
        }

        protected void OverrideInstallerPropsFromAppConfig()
        {
            // get the path, since out current directory is not the one where .EXE resides
            var mainAssembly = GetType().Assembly; //Assembly.GetExecutingAssembly();
            var commonAssembly = typeof(WindowsServiceConfigSection).Assembly;
            var path = mainAssembly.Location;

            // Get the configuration file.
            var configuration = ConfigurationManager.OpenExeConfiguration(path);

            // Make current assembly reachable for resolution, since it is not available in installer's context by default,
            // and we need current assembly for the custom configuration section type
            AppDomain.CurrentDomain.AssemblyResolve +=
                (sender, args) => GetAssembly(args.Name, mainAssembly, commonAssembly);

            // Get the host settings section.
            var hostSettings = (WindowsServiceConfigSection)configuration.GetSection("thinksmartHost");

            if (hostSettings != null)
            {
                if (!string.IsNullOrEmpty(hostSettings.WindowsServiceName))
                {
                    serviceInstaller.ServiceName = hostSettings.WindowsServiceName;
                }
                if (!string.IsNullOrEmpty(hostSettings.WindowsServiceDisplayName))
                {
                    serviceInstaller.DisplayName = hostSettings.WindowsServiceDisplayName;
                }
                if (!string.IsNullOrEmpty(hostSettings.WindowsServiceDescription))
                {
                    serviceInstaller.Description = hostSettings.WindowsServiceDescription;
                }
            }
            else
            {
                throw new Exception("Could not locate thinksmartHost configuration section in config file!");
            }
        }

        private Assembly GetAssembly(string name, Assembly mainAssembly, Assembly commonAssembly)
        {
            if (name == mainAssembly.GetName().Name)
                return mainAssembly;

            if (name == commonAssembly.GetName().Name)
                return commonAssembly;

            return null;
        }

        protected string GetParamOrDefault(string paramName, string defaultValue)
        {
            var args = Environment.GetCommandLineArgs();
            var prefix = "/" + paramName + "=";
            for (var i = 1; i < args.Length; i++)
            {
                if (args[i].StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    return args[i].Substring(prefix.Length);
                }
            }

            return defaultValue;
        }
    }
}
