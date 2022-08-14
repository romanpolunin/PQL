namespace Pql.SqlEngine.Interfaces
{
    /// <summary>
    /// External logger interface.
    /// </summary>
    public interface ITracer
    {
        /// <summary>
        /// True if Debug level is enabled for this tracer object.
        /// </summary>
        bool IsDebugEnabled { get; }

        /// <summary>
        /// True if Info level is enabled for this tracer object.
        /// </summary>
        bool IsInfoEnabled { get; }

        /// <summary>
        /// Logs a message with level Debug.
        /// </summary>
        void Debug(string message);

        /// <summary>
        /// Logs a message with level Info.
        /// </summary>
        void Info(string message);

        /// <summary>
        /// Logs a message with level Info.
        /// </summary>
        void InfoFormat(string message, params object[] args);

        /// <summary>
        /// Logs a message with level Error.
        /// </summary>
        void Exception(Exception exception);

        /// <summary>
        /// Logs a message with level Error.
        /// </summary>
        /// <param name="message">Additional message to log before exception</param>
        /// <param name="exception">Exception to log</param>
        void Exception(string message, Exception exception);

        /// <summary>
        /// Logs a message with level Fatal.
        /// </summary>
        /// <param name="message">Additional message to log before exception</param>
        /// <param name="exception">Exception to log</param>
        void Fatal(string message, Exception exception);
    }
}
