using System;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Implementation of client security context, as it gets passed between client and server.
    /// </summary>
    public class PqlClientSecurityContext : IPqlClientSecurityContext
    {
        /// <summary>
        /// Identifier of the current tenant.
        /// </summary>
        public string TenantId { get; private set; }

        /// <summary>
        /// Identifier of the current user.
        /// </summary>
        public string UserId { get; private set; }

        /// <summary>
        /// Name of the client application.
        /// </summary>
        public string ApplicationName { get; private set; }

        /// <summary>
        /// Identifier of the security context usually migrates over all layers.
        /// </summary>
        public string ContextId { get; private set; }

        /// <summary>
        /// Ctr.
        /// </summary>
        public PqlClientSecurityContext(string contextId, string applicationName, string tenantId, string userId)
        {
            if (string.IsNullOrEmpty(contextId))
            {
                throw new ArgumentNullException("contextId");
            }

            if (string.IsNullOrEmpty(applicationName))
            {
                throw new ArgumentNullException("applicationName");
            }

            if (string.IsNullOrEmpty(tenantId))
            {
                throw new ArgumentNullException("tenantId");
            }

            if (string.IsNullOrEmpty(userId))
            {
                throw new ArgumentNullException("userId");
            }

            ContextId = contextId;
            ApplicationName = applicationName;
            TenantId = tenantId;
            UserId = userId;
        }
    }
}