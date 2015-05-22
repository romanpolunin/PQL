using System;
using Pql.Engine.Interfaces;

namespace Pql.IntegrationStubs
{
    public interface IHostingService
    {
        void HandleHostedProcessFailure(IHostedProcess sender, string reason);
        ITracer GetTracer(Type getType);
    }
}