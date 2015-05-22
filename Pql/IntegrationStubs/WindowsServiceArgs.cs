using System;
using System.Collections.Generic;

namespace Pql.IntegrationStubs
{
    public class HostedProcessArgs : IHostedProcessArgs
    {
        public HostedProcessArgs()
        {
            CommandLineArgs = Environment.GetCommandLineArgs();
            IsInteractive = Environment.UserInteractive;

            foreach (var arg in CommandLineArgs)
            {
                if (arg.StartsWith("/") || arg.StartsWith("-"))
                {
                    switch (arg.Substring(1).ToLower())
                    {
                        case "c":
                        case "console":
                            IsInteractive = true;
                            break;
                        case "?":
                        case "h":
                        case "help":
                            Help = true;
                            break;
                        case "i":
                        case "install":
                            Install = true;
                            break;
                        case "u":
                        case "uninstall":
                            Uninstall = true;
                            break;
                    }
                }
            }
        }

        public IReadOnlyList<string> CommandLineArgs { get; private set; }
        public bool IsInteractive { get; private set; }
        public bool Help { get; private set; }
        public bool Install { get; private set; }
        public bool Uninstall { get; private set; }
    }
}