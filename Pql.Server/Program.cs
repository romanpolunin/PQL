using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Pql.Server;

var loggerFactory = LoggerFactory.Create(x =>
{

});

var builder = new HostBuilder()
    .ConfigureServices(services =>
    {
        services.AddSingleton(loggerFactory);
    })
    .ConfigureWebHostDefaults(webHost =>
    {
        webHost.UseStartup<Startup>();
    });

var host = builder.Start();
