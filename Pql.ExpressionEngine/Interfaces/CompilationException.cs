using System.Runtime.Serialization;

using Irony.Parsing;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// Compilation exception.
    /// </summary>
    [Serializable]
    public class CompilationException : Exception
    {
        /// <summary>
        /// Ctr.
        /// </summary>
        public CompilationException(string message) : base(message)
        {
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public CompilationException(string message, ParseTreeNode? location) : base(CreateErrorMessage(message, location))
        {

        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public CompilationException(ParseTreeNode? location)
            : base(CreateErrorMessage(location))
        {

        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public CompilationException(SerializationInfo serializationInfo, StreamingContext streamingContext)
            : base(serializationInfo, streamingContext)
        {

        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public static string CreateErrorMessage(string message, ParseTreeNode? location)
        {
            var defaultMessage = CreateErrorMessage(location);
            return string.IsNullOrEmpty(message) ? defaultMessage : defaultMessage + " " + message;
        }

        /// <summary>
        /// Formats an error message based on given location.
        /// </summary>
        /// <param name="location">Optional location</param>
        public static string CreateErrorMessage(ParseTreeNode? location)
        {
            if (location == null)
            {
                return "Syntax error at unknown location";
            }

            var loc = location.Span.Location;
            var text = location.Term == null ? string.Empty : (" Term: " + location.Term.Name + ".");
            return string.Format("Syntax error at {0}.{1}", FormatLocationString(loc, location.Span.Length), text);
        }

        /// <summary>
        /// Formats location string.
        /// </summary>
        public static string FormatLocationString(SourceLocation loc, int length)
        {
            return string.Format("pos {0}, line {1}, col {2}, length {3}", loc.Position, loc.Line, loc.Column, length);
        }
    }
}