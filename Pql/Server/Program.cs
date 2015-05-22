using System.Reflection;
using Pql.IntegrationStubs;

namespace Pql.Server
{
    internal class Program : ProgramBase
    {
        private static int Main(string[] args)
        {
            return MainImpl(args, Assembly.GetExecutingAssembly());
        }
    }
}
