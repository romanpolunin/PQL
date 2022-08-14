namespace Pql.SqlEngine.Interfaces.Services
{
    /// <summary>
    /// Type of a single change record.
    /// </summary>
    public enum DriverChangeType
    {
        /// <summary>
        /// Insertion.
        /// </summary>
        Insert = 1,
        /// <summary>
        /// Update of an existing record.
        /// </summary>
        Update = 2,
        /// <summary>
        /// Deletion of an existing record.
        /// </summary>
        Delete = 3
    }
}