using System;

namespace Pql.Engine.Interfaces.Services
{
    /// <summary>
    /// Storage driver factory is plugged into PQL query processor core, 
    /// provides instances of configuration and drivers.
    /// </summary>
    public interface IStorageDriverFactory
    {
        /// <summary>
        /// Creates a new uninitialized instance of a storage driver.
        /// </summary>
        /// <seealso cref="GetDriverConfig"/>
        IStorageDriver Create();

        /// <summary>
        /// Reads configuration settings object for a particular scope.
        /// Settings object type is specific to implementation.
        /// </summary>
        /// <param name="scopeId">ScopeId is used to locate data store</param>
        /// <returns></returns>
        object GetDriverConfig(string scopeId);

        /// <summary>
        /// Returns storage format version as of this particular assembly.
        /// </summary>
        /// <seealso cref="MinCompatibleStoreVersion"/>
        Version CurrentStoreVersion();

        /// <summary>
        /// Returns minimum storage version this factory and its drivers are compatible with.
        /// </summary>
        /// <seealso cref="CurrentStoreVersion"/>
        Version MinCompatibleStoreVersion();
    }
}