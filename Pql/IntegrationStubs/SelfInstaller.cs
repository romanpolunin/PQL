using System;
using System.Collections.Generic;
using System.Configuration.Install;

namespace Pql.IntegrationStubs
{
    public static class SelfInstaller
    {
        public static void InstallMe()
        {
            var args = CreateInstallerArgs("/i");

            ManagedInstallerClass.InstallHelper(args.ToArray());
        }

        public static void UninstallMe()
        {
            var args = CreateInstallerArgs("/u");

            ManagedInstallerClass.InstallHelper(args.ToArray());
        }

        private static List<string> CreateInstallerArgs(params string[] otherArgs)
        {
            var args = new List<string>();
            args.AddRange(Environment.GetCommandLineArgs());

            if (otherArgs != null)
            {
                args.AddRange(otherArgs);
            }

            var exeLocation = args[0];
            args.RemoveAt(0);
            args.Add(exeLocation);

            return args;
        }
    }
}
