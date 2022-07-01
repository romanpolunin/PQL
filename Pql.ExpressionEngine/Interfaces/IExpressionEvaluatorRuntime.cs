using System.Linq.Expressions;

using Irony.Parsing;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// Interface of expression evaluation factory. 
    /// All methods are thread-safe and compiled lambdas.
    /// Thread safety guarantee does not cover custom extensions.
    /// Parsing may incur some waiting if number of concurrent parsing operations exceeds Environment.ProcessorCount * 2.
    /// </summary>
    public interface IExpressionEvaluatorRuntime
    {
        /// <summary>
        /// Registers at atom to process identifiers whose names are not known at the time of runtime initialization.
        /// Atom will be used as a handler and must have non-null <see cref="AtomMetadata.ExpressionGenerator"/> member.
        /// This handler will be invoked from inside <see cref="Analyze"/> for identifiers which are not statically known atoms.
        /// More than one handler can be registered, they will be invoked one after another in arbitrary order 
        /// until one of their expression generators returns a non-null value.
        /// </summary>
        /// <param name="atom">An atom to register. Must have non-null <see cref="AtomMetadata.ExpressionGenerator"/> member</param>
        /// <see cref="RegisterAtom"/>
        /// <see cref="AtomMetadata.ExpressionGenerator"/>
        void RegisterDynamicAtomHandler(AtomMetadata atom);

        /// <summary>
        /// Registers a new atom. This API is for statically known identifiers and functions.
        /// To process identifiers whose names are not known at the time of runtime init use <see cref="RegisterDynamicAtomHandler"/>.
        /// </summary>
        /// <param name="atom">An atom to register</param>
        /// <seealso cref="AtomMetadata"/>
        /// <seealso cref="RegisterDynamicAtomHandler"/>
        void RegisterAtom(AtomMetadata atom);

        /// <summary>
        /// Checks if an atom with this name is already registered.
        /// </summary>
        /// <param name="name">Name to look for</param>
        /// <returns>True if an atom with this name is already registered</returns>
        bool IsAtomRegistered(string name);

        /// <summary>
        /// Parses specified expression text, with a cancellation option. 
        /// May incur some waiting in highly concurrent environment, because the number of pooled parsers is limited.
        /// </summary>
        /// <param name="text">Expression text</param>
        /// <param name="cancellation">Cancellation token source to be used to abort waiting. Supply null to wait indefinitely.</param>
        /// <returns>Abstract syntax tree</returns>
        /// <exception cref="OperationCanceledException">Aborted while waiting for some parser to become available</exception>
        /// <exception cref="CompilationException">Failed to parse, message contains details from parser</exception>
        ParseTree Parse(string text, CancellationToken cancellation);

        /// <summary>
        /// Produces .NET Expression object from the given abstract syntax tree.
        /// Supports re-entrancy, useful for expression generators (see <see cref="AtomMetadata.ExpressionGenerator"/>).
        /// </summary>
        /// <param name="root">Parse tree root</param>
        /// <param name="state">Compiler state</param>
        /// <returns>Expression node with verified logical data type (may not match return type specified in CompilerState)</returns>
        /// <exception cref="ArgumentNullException"><paramref name="root"/> is null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="state"/> is null</exception>
        /// <exception cref="CompilationException">Some errors in the compilation</exception>
        /// <seealso cref="AdjustReturnType"/>
        Expression Analyze(ParseTreeNode root, CompilerState state);

        /// <summary>
        /// If return type of the given <paramref name="expression"/> does not match <paramref name="type"/>,
        /// attempts to adjust it. Returns same or new Expression object.
        /// If supplied type is <c>null</c>, expression becomes Action.
        /// </summary>
        /// <param name="expression">Parse tree root</param>
        /// <param name="type">Desired return type, or null to produce an action</param>
        /// <exception cref="ArgumentNullException"><paramref name="expression"/> is null</exception>
        /// <exception cref="CompilationException">Could produce a safe adjustment</exception>
        Expression AdjustReturnType(Expression expression, Type type);

        /// <summary>
        /// Parses, analyzes and compiles given expression string into a CLR lambda of appropriate type (some flavor of Func or Action).
        /// May incur some waiting in highly concurrent environment, because the number of pooled parsers is limited.
        /// </summary>
        /// <param name="text">Expression text</param>
        /// <param name="returnType">Desired return type - used for verification. Supply null to compile to Action</param>
        /// <param name="args">Ordered name-type pairs of input arguments</param>
        /// <returns>Compiled lambda, some flavor of Func or Action</returns>
        /// <exception cref="ArgumentNullException"><paramref name="text"/> is null</exception>
        /// <exception cref="CompilationException">Some errors in the compilation</exception>
        object Compile(string text, Type returnType, params Tuple<string, Type>[] args);

        /// <summary>
        /// Parses, analyzes and compiles given expression string into a CLR lambda of appropriate type (some flavor of Func or Action).
        /// May incur some waiting in highly concurrent environment, because the number of pooled parsers is limited.
        /// Has cancellation option.
        /// </summary>
        /// <param name="text">Expression text</param>
        /// <param name="cancellation">Cancellation token source to be used to abort waiting. Supply null to wait indefinitely.</param>
        /// <param name="returnType">Desired return type - used for verification. Supply null to compile to Action</param>
        /// <param name="args">Ordered name-type pairs of input arguments</param>
        /// <returns>Compiled lambda, some flavor of Func or Action</returns>
        /// <exception cref="ArgumentNullException"><paramref name="text"/> is null</exception>
        /// <exception cref="CompilationException">Some errors in the compilation</exception>
        object Compile(string text, CancellationToken cancellation, Type returnType, params Tuple<string, Type>[] args);

        /// <summary>
        /// Parses, analyzes and compiles given expression string into a CLR lambda of appropriate type (some flavor of Func or Action).
        /// May incur some waiting in highly concurrent environment, because the number of pooled parsers is limited.
        /// </summary>
        /// <param name="text">Expression text</param>
        /// <typeparam name="TContext">Desired input holder type, will be the only input argument. Available in <seealso cref="CompilerState.Arguments"/> by name "Context"</typeparam>
        /// <typeparam name="T">Desired return type, used for verification</typeparam>
        /// <returns>Compiled Func</returns>
        /// <exception cref="ArgumentNullException"><paramref name="text"/> is null</exception>
        /// <exception cref="CompilationException">Some errors in the compilation</exception>
        Func<TContext, T> Compile<TContext, T>(string text);

        /// <summary>
        /// Parses, analyzes and compiles given expression string into a CLR lambda of appropriate type (some flavor of Func or Action).
        /// May incur some waiting in highly concurrent environment, because the number of pooled parsers is limited.
        /// Has cancellation option.
        /// </summary>
        /// <param name="text">Expression text</param>
        /// <param name="cancellation">Cancellation token source to be used to abort waiting. Supply null to wait indefinitely.</param>
        /// <typeparam name="TContext">Desired input holder type, will be the only input argument. Available in <seealso cref="CompilerState.Arguments"/> by name "Context"</typeparam>
        /// <typeparam name="T">Desired return type, used for verification</typeparam>
        /// <returns>Compiled Func</returns>
        /// <exception cref="ArgumentNullException"><paramref name="text"/> is null</exception>
        /// <exception cref="CompilationException">Some errors in the compilation</exception>
        Func<TContext, T> Compile<TContext, T>(string text, CancellationToken cancellation);

        /// <summary>
        /// Given an expression (assuming it was generated with <see cref="Analyze"/>), attempts to compile it into a lambda of some type.
        /// </summary>
        /// <param name="expression">Expression</param>
        /// <param name="state">Compiler state</param>
        /// <returns>Compiled Func</returns>
        /// <exception cref="ArgumentNullException"><paramref name="expression"/> is null</exception>
        /// <exception cref="ArgumentNullException"><paramref name="state"/> is null</exception>
        /// <exception cref="CompilationException">Mismatch in argument or context types, other consistency errors</exception>
        object Compile(Expression expression, CompilerState state);
    }
}