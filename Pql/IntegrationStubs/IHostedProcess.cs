using System;

namespace Pql.IntegrationStubs
{
    public interface IHostedProcess : IDisposable
    {
        void Start(IHostingService host);
        void Pause();
        void Continue();
    }
}
