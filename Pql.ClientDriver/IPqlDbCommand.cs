using System.Data;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Contract for DbCommand extensions for PQL server.
    /// </summary>
    public interface IPqlDbCommand : IDbCommand
    {
        /// <summary>
        /// Overload of <see cref="IDbCommand.ExecuteNonQuery"/> for bulk operations.
        /// </summary>
        /// <param name="argCount">Number of items in <paramref name="bulkArgs"/></param>
        /// <param name="bulkArgs">Data to be streamed to server, one row at a time. May be same object with different values. 
        /// Must match fields number and order specified by <paramref name="fieldNames"/></param>
        /// <param name="entityName">Name of the entity to be run operation on</param>
        /// <param name="fieldNames">Names of fields to be selected, inserted or updated. Must include primary key field for all modification commands, optional for select</param>
        /// <returns>Number of records selected or affected</returns>
        /// <exception cref="ArgumentNullException"><paramref name="bulkArgs"/> may not be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="fieldNames"/> may not be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="entityName"/> may not be null</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="argCount"/> has invalid value</exception>
        int BulkInsert(string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> bulkArgs);

        /// <summary>
        /// Overload of <see cref="IDbCommand.ExecuteNonQuery"/> for bulk operations.
        /// </summary>
        /// <param name="argCount">Number of items in <paramref name="requestBulk"/></param>
        /// <param name="requestBulk">Data to be streamed to server, one row at a time. May be same object with different values. 
        /// Must match fields number and order specified by <paramref name="fieldNames"/></param>
        /// <param name="entityName">Name of the entity to be run operation on</param>
        /// <param name="fieldNames">Names of fields to be selected, inserted or updated. Must include primary key field for all modification commands, optional for select</param>
        /// <returns>Number of records selected or affected</returns>
        /// <exception cref="ArgumentNullException"><paramref name="requestBulk"/> may not be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="fieldNames"/> may not be null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="entityName"/> may not be null</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="argCount"/> has invalid value</exception>
        int BulkUpdate(string entityName, string[] fieldNames, int argCount, IEnumerable<RowData> requestBulk);
    }
}