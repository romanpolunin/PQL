using System.Collections.Generic;

namespace Pql.IntegrationStubs
{
    public interface IHostedProcessArgs
    {
        IReadOnlyList<string> CommandLineArgs { get; }
        bool IsInteractive { get; }
        bool Help { get; }
        bool Install { get; }
        bool Uninstall { get; }
    }
}