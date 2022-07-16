using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Grpc.Net.Client;

using Microsoft.Extensions.Logging;

namespace Pql.ClientDriver.Tests
{
    public class IntegrationTestBase<TServiceImpl> : IDisposable where TServiceImpl: class
    {
        private GrpcChannel? _channel;
        private readonly IDisposable? _testContext;

        protected GrpcTestFixture<Startup<TServiceImpl>> Fixture { get; set; }

        protected GrpcChannel Channel => _channel ??= CreateChannel();

        protected GrpcChannel CreateChannel()
        {
            return GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
            {
                LoggerFactory = Fixture.LoggerFactory,
                HttpHandler = Fixture.Handler
            });
        }

        public IntegrationTestBase(GrpcTestFixture<Startup<TServiceImpl>> fixture)
        {
            Fixture = fixture;
            _testContext = Fixture?.GetTestContext();
        }

        public void Dispose()
        {
            _testContext?.Dispose();
            _channel = null;
        }
    }
}