namespace Pql.ClientDriver.Protocol
{
    /// <summary>
    /// Utility to write PQL authentication information, including tenantId, userId etc.
    /// Captures several properties of <see cref="IPqlClientSecurityContext"/>.
    /// </summary>
    public static class AuthContextSerializer
    {
        /// <summary>
        /// Writes <see cref="IPqlClientSecurityContext"/> to semicolon-separated string.
        /// </summary>
        public static string GetString(IPqlClientSecurityContext ctx) => string.Format("{0};{1};{2};{3}", ctx.ContextId, ctx.ApplicationName, ctx.TenantId, ctx.UserId);

        /// <summary>
        /// Reads <see cref="IPqlClientSecurityContext"/> from a semicolon-separated string.
        /// </summary>
        public static IPqlClientSecurityContext GetObject(string data)
        {
            if (string.IsNullOrEmpty(data))
            {
                throw new ArgumentNullException(nameof(data));
            }
            
            var parts = data.Split(new [] {';'}, StringSplitOptions.None);
            if (parts.Length != 4)
            {
                throw new ArgumentException("Data must have exactly 4 semicolon-separated values", nameof(data));
            }

            var result = new PqlClientSecurityContext(parts[0], parts[1], parts[2], parts[3]);
            return result;
        }
    }
}
