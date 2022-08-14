using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.Interfaces.Services
{
    /// <summary>
    /// A contract for data enumerator that puts every row into an instance of <see cref="DriverRowData"/>.
    /// Used in various contexts such as base dataset iteration, integration with storage driver, fetching input data for bulk operations.
    /// </summary>
    public interface IDriverDataEnumerator : IDisposable
    {
        /// <summary>
        /// Moves to a next record and updates content of <see cref="Current"/>.
        /// </summary>
        /// <returns>True if successfully moved to next record and <see cref="Current"/> can return something.</returns>
        bool MoveNext();

        /// <summary>
        /// When <see cref="MoveNext"/> is called, enumerator implementation may choose to only fetch a subset of fields,
        /// for instance only those that client asked about. This small subset of fields may be used to compute WHERE clause etc.
        /// However if client decides that the record "is good", it may ask the enumerator to fetch remaining values.
        /// </summary>
        void FetchAdditionalFields();

        /// <summary>
        /// Current data item. Only meaningful if a previous call to <see cref="MoveNext"/> returned true.
        /// </summary>
        DriverRowData Current { get; }

        /// <summary>
        /// Initializes <see cref="DriverChangeBuffer.InternalEntityId"/> field.
        /// This will either take internal entity id from current data row
        /// or from the computed clause evaluaton context (for non-bulk inserts)
        /// </summary>
        void FetchInternalEntityIdIntoChangeBuffer(DriverChangeBuffer changeBuffer, RequestExecutionContext context);
    }
}