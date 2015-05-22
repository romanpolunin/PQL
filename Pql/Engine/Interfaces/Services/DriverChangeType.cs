namespace Pql.Engine.Interfaces.Services
{
    /// <summary>
    /// Type of a single change record.
    /// </summary>
    public enum DriverChangeType
    {
        /// <summary>
        /// Dummy default value.
        /// </summary>
        InvalidValue,
        /// <summary>
        /// Insertion.
        /// </summary>
        Insert,
        /// <summary>
        /// Update of an existing record.
        /// </summary>
        Update,
        /// <summary>
        /// Deletion of an existing record.
        /// </summary>
        Delete
    }
}