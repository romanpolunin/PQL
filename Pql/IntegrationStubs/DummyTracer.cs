using System;
using Pql.Engine.Interfaces;

namespace Pql.IntegrationStubs
{
    public class DummyTracer : ITracer
    {
        public bool IsDebugEnabled { get; private set; }
        public bool IsInfoEnabled { get; private set; }
        public void Debug(string message)
        {
            Console.WriteLine("DEBUG: " + message);
        }

        public void Info(string message)
        {
            Console.WriteLine("INFO: " + message);
        }

        public void InfoFormat(string message, params object[] args)
        {
            Console.WriteLine("INFO: " + message, args);
        }

        public void Exception(Exception exception)
        {
            Console.WriteLine("ERROR: " + exception.ToString());
        }

        public void Exception(string message, Exception exception)
        {
            Console.WriteLine("ERROR: " + message + Environment.NewLine + exception);
        }

        public void Fatal(string message, Exception exception)
        {
            Console.WriteLine("FATAL: " + message + Environment.NewLine + exception);
        }
    }
}