using System.Diagnostics;
using Pql.IntegrationStubs;

namespace Pql.Server.UnitTest
{
    public class DummyHostedProcess : IHostedProcess 
    {
        public void Dispose()
        {
        }

        public void Start(IHostingService host)
        {
            Debug.WriteLine("Starting");
        }

        public void Pause()
        {
            throw new System.NotImplementedException();
        }

        public void Continue()
        {
            throw new System.NotImplementedException();
        }

        public void HandleFailNode(string reason)
        {
            Debug.WriteLine("HandleFailNode: " + reason);
        }
    }
}