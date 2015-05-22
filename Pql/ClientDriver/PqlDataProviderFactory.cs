using System.Data.Common;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Integration with ADO.NET framework, used in machine.config under system.data/DbProviderFactories section.
    /// </summary>
    public sealed class PqlDataProviderFactory : DbProviderFactory
    {
        /// <summary>
        /// Required by ADO.NET infrastructure (<see cref="DbProviderFactories.GetFactory(string)"/> will read it when spawning a provider).
        /// </summary>
        public static readonly PqlDataProviderFactory Instance = new PqlDataProviderFactory();

        /// <summary>
        /// Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbCommand"/> class.
        /// </summary>
        /// <returns>
        /// A new instance of <see cref="T:System.Data.Common.DbCommand"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override DbCommand CreateCommand()
        {
            return new PqlDataCommand(null);
        }

        /// <summary>
        /// Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbConnection"/> class.
        /// </summary>
        /// <returns>
        /// A new instance of <see cref="T:System.Data.Common.DbConnection"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override DbConnection CreateConnection()
        {
            return new PqlDataConnection();
        }

        /// <summary>
        /// Returns a new instance of the provider's class that implements the <see cref="T:System.Data.Common.DbParameter"/> class.
        /// </summary>
        /// <returns>
        /// A new instance of <see cref="T:System.Data.Common.DbParameter"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override DbParameter CreateParameter()
        {
            return new PqlDataCommandParameter();
        }
    }
}
