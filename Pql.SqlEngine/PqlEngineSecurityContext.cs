using Pql.SqlEngine.Interfaces;

namespace Pql.Engine.DataContainer
{
    /// <summary>
    /// Manages request-specific security context on a thread that processes this request.
    /// </summary>
    public static class PqlEngineSecurityContext
    {
        private static readonly ThreadLocal<IPqlClientSecurityContext> s_context = new ThreadLocal<IPqlClientSecurityContext>();

        /// <summary>
        /// Sets security context on current thread's state.
        /// </summary>
        public static void Set(IPqlClientSecurityContext clientSecurityContext)
        {
            s_context.Value = clientSecurityContext ?? throw new ArgumentNullException(nameof(clientSecurityContext));
        }
    }
}