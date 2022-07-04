namespace Pql.ClientDriver
{
    /// <summary>
    /// Defines client-side security context that supplies data 
    /// for request security descriptor.
    /// </summary>
    public interface IPqlClientSecurityContext
    {
        /// <summary>
        /// Identifier of the current tenant.
        /// </summary>
        string TenantId { get; }
        /// <summary>
        /// Identifier of the current user.
        /// </summary>
        string UserId { get; }
        /// <summary>
        /// Name of the client application.
        /// </summary>
        string ApplicationName { get; }
        /// <summary>
        /// Identifier of the security context usually migrates over all layers.
        /// </summary>
        string ContextId { get; }
    }
}