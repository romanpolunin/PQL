using System;
using System.ComponentModel;
using Pql.Engine.Interfaces.Internal;
using Pql.IntegrationStubs;

namespace Pql.Server
{
    [RunInstaller(true)]
    public class ServiceInstaller : ServiceInstallerBase
    {
        public ServiceInstaller()
        {
            serviceInstaller.Description = GetParamOrDefault("serviceDesc", "PqlServer");
            serviceInstaller.DisplayName = GetParamOrDefault("serviceDisplayName", "PqlServer");
            serviceInstaller.ServiceName = GetParamOrDefault("serviceName", "PQLSERVER");
            serviceInstaller.StartType = (System.ServiceProcess.ServiceStartMode)Enum.Parse(
               typeof(System.ServiceProcess.ServiceStartMode), GetParamOrDefault("serviceStartMode", "Automatic"));

            serviceProcessInstaller.Account = (System.ServiceProcess.ServiceAccount)Enum.Parse(
               typeof(System.ServiceProcess.ServiceAccount), GetParamOrDefault("serviceAccountType", "LocalSystem"));
            serviceProcessInstaller.Password = GetParamOrDefault("serviceAccountPassword", null);
            serviceProcessInstaller.Username = GetParamOrDefault("serviceAccountName", null);

            OverrideInstallerPropsFromAppConfig();
        }

        protected override void OnAfterInstall(System.Collections.IDictionary savedState)
        {
            PerfCounters.Install();

            base.OnAfterInstall(savedState);
        }

        protected override void OnAfterUninstall(System.Collections.IDictionary savedState)
        {
            PerfCounters.Remove();

            base.OnAfterUninstall(savedState);
        }
    }
}
