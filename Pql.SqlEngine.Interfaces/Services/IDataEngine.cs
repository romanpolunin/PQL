using System;
using Pql.Engine.Interfaces.Internal;

namespace Pql.Engine.Interfaces.Services
{
    /// <summary>
    /// Contract for query processing engine.
    /// </summary>
    public interface IDataEngine : IDisposable
    {
        /// <summary>
        /// Begin data production. 
        /// Reponsible for production of data stream, chunking it and putting chunks into <see cref="RequestExecutionContext.BuffersRing"/>.
        /// </summary>
        /// <seealso cref="EndExecution"/>
        void BeginExecution(RequestExecutionContext context);
        /// <summary>
        /// Terminates production thread if it is still running.
        /// </summary>
        /// <seealso cref="BeginExecution"/>
        void EndExecution(RequestExecutionContext context, bool waitForProducerThread);

        /// <summary>
        /// UTC date and time of last time when this engine's <see cref="BeginExecution"/> method was invoked.
        /// </summary>
        DateTime UtcLastUsedAt { get; }
        
        /// <summary>
        /// Asks the engine to write its current state information to trace log.
        /// </summary>
        void WriteStateInfoToLog();
    }
}