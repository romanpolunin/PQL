using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.Interfaces.Services
{
    /// <summary>
    /// Storage driver contract abstracts PQL query processor core from underlying data store's representation format and location
    /// and guarantees that any data source can be plugged into PQL query processor core.
    /// </summary>
    public interface IStorageDriver : IDisposable
    {
        /// <summary>
        /// Initializes driver with settings object.
        /// </summary>
        /// <param name="tracer">Tracer sink</param>
        /// <param name="settings">Arbitrary settings object, type specific to implementation</param>
        /// <seealso cref="IStorageDriverFactory.Create"/>
        /// <seealso cref="IStorageDriverFactory.GetDriverConfig"/>
        void Initialize(ITracer tracer, object settings);

        /// <summary>
        /// Reads data container descriptor from underlying store.
        /// </summary>
        DataContainerDescriptor GetDescriptor();
        
        /// <summary>
        /// Reads statistics about underlying store.
        /// </summary>
        DataContainerStats GetStats();

        /// <summary>
        /// Asks driver to make sure that column data for specified field is available for querying and modification.
        /// This
        /// </summary>
        void BeginPrepareColumnData(int fieldId);

        /// <summary>
        /// Makes sure that all columns' data structures on a specified document type 
        /// are ready for querying and modification when this method returns. 
        /// </summary>
        void PrepareAllColumnsAndWait(int docType);
        
        /// <summary>
        /// Enumerates through data for a particular SELECT query.
        /// Inside MoveNext() imlementation, MUST populate the same instance of row data, pointed to by <see cref="RequestExecutionContext.DriverOutputBuffer"/>.
        /// Yields dummy true value, its value and data type are reserved for future use.
        /// </summary>
        IDriverDataEnumerator GetData(RequestExecutionContext context);

        /// <summary>
        /// Creates an internal buffer for changes and returns a handle to it.
        /// </summary>
        /// <param name="changeBuffer">Reference to change buffer that will be used to add changes</param>
        /// <param name="isBulk">Tells the driver that changeset is expected to have large number of changes. 
        /// Driver may choose to take some locks immediately and release them only when changeset is applied of discarded,
        /// instead of taking locks for every individual change.</param>
        /// <seealso cref="AddChange"/>
        long CreateChangeset(DriverChangeBuffer changeBuffer, bool isBulk);
        
        /// <summary>
        /// Takes information about changed data from <see cref="DriverChangeBuffer"/> initialized with <see cref="CreateChangeset"/> and writes it into underlying store.
        /// May buffer changes in memory. Client calls <see cref="Apply"/> to make sure everything is synced to store.
        /// </summary>
        /// <seealso cref="CreateChangeset"/>
        void AddChange(long changeset);

        /// <summary>
        /// Flushes pending changeset to underlying store.
        /// Returns number of effectively applied changes.
        /// </summary>
        /// <seealso cref="CreateChangeset"/>
        int Apply(long changeset);

        /// <summary>
        /// Discards a pending changeset, if it still exists.
        /// </summary>
        /// <seealso cref="CreateChangeset"/>
        void Discard(long changeset);

        /// <summary>
        /// Performs compaction: physical removal of deleted entries and rebuild of sorting indexes.
        /// </summary>
        /// <seealso cref="CompactionOptions"/>
        void Compact(CompactionOptions options);

        /// <summary>
        /// Makes sure that all modified records are persisted in the underlying storage media.
        /// </summary>
        void FlushDataToStore();

        /// <summary>
        /// Returns true if this driver supports updating values of this field.
        /// </summary>
        bool CanUpdateField(int fieldId);

        /// <summary>
        /// Asks driver to preallocate storage capacity for required number of documents of certain type.
        /// Used for bulk insertion.
        /// </summary>
        void AllocateCapacityForDocumentType(int documentType, int additionalCapacity);
    }
}