/*
using System;
using System.Threading;
using Pql.ClientDriver;

namespace Pql.Engine.DataContainer
{
    /// <summary>
    /// Manages request-specific security context on a thread that processes this request.
    /// </summary>
    public static class PqlEngineSecurityContext
    {
        private static readonly ThreadLocal<IPqlClientSecurityContext> Context = new ThreadLocal<IPqlClientSecurityContext>();

        /// <summary>
        /// Sets security context on current thread's state.
        /// </summary>
        public static void Set(IPqlClientSecurityContext clientSecurityContext)
        {
            Context.Value = clientSecurityContext ?? throw new ArgumentNullException("clientSecurityContext");
        }
    }
}
*/