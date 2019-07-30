using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// Utility class used by compiler during parsing-analysis-compilation sequence.
    /// API consumers may extend this class and provide their own instance into Analyze and Compile methods.
    /// </summary>
    public class CompilerState
    {
        /// <summary>
        /// Reference to the runtime which created this state object.
        /// Useful for implementors of <see cref="AtomMetadata.ExpressionGeneratorCallback"/>, to perform re-entrant calls.
        /// </summary>
        /// <see cref="AtomMetadata.ExpressionGeneratorCallback"/>
        public readonly IExpressionEvaluatorRuntime ParentRuntime;

        /// <summary>
        /// Argument expression, reference to the value provider object.
        /// </summary>
        public readonly IReadOnlyList<ParameterExpression> Arguments;

        /// <summary>
        /// Desired return type of the resulting compiled lambda expression.
        /// </summary>
        public readonly Type ReturnType;

        /// <summary>
        /// Return type as derived from the source Expression.
        /// </summary>
        public Type RawReturnType;

        /// <summary>
        /// Set to true to request an Action delegate to be built instead of a Func.
        /// </summary>
        public bool CompileToAction;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="parentRuntime">Reference to the runtime which created this state object</param>
        /// <param name="contextType">Optional, type of the default @Context argument</param>
        /// <param name="returnType">Optional return type, supply null to compile to Action</param>
        /// <param name="args">Ordered list of types of arguments</param>
        public CompilerState(IExpressionEvaluatorRuntime parentRuntime, Type contextType, Type returnType, params Tuple<string, Type>[] args)
        {
            var arguments = new List<ParameterExpression>();

            if (contextType != null)
            {
                arguments.Add(Expression.Parameter(contextType, "@Context"));
            }

            if (args != null && args.Length > 0)
            {
                for (var i = 0; i < args.Length; i++)
                {
                    var name = args[i].Item1;
                    var type = args[i].Item2;

                    if (string.IsNullOrEmpty(name))
                    {
                        throw new ArgumentException("Empty argument name at position " + i);
                    }

                    if (arguments.Any(x => 0 == StringComparer.OrdinalIgnoreCase.Compare(x.Name, name)))
                    {
                        throw new ArgumentException("Duplicate argument name: " + name);
                    }

                    arguments.Add(Expression.Parameter(type, name));
                }
            }

            ParentRuntime = parentRuntime ?? throw new ArgumentNullException("parentRuntime");
            ReturnType = returnType;
            Arguments = arguments;
        }

        /// <summary>
        /// Finds an argument by name.
        /// </summary>
        /// <param name="name">Name to look for (case-insensitive).</param>
        /// <returns>Parameter expression</returns>
        /// <exception cref="ArgumentException">Argument with this name is not defined</exception>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is null or empty</exception>
        public ParameterExpression GetArgumentByName(string name)
        {
            var result = TryGetArgumentByName(name);
            if (result == null)
            {
                throw new ArgumentException("Argument " + name + "is not defined");
            }

            return result;
        }

        /// <summary>
        /// Finds an argument by name.
        /// </summary>
        /// <param name="name">Name to look for (case-insensitive).</param>
        /// <returns>Parameter expression or null if not found</returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is null or empty</exception>
        public ParameterExpression TryGetArgumentByName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("name");
            }

            foreach (var item in Arguments)
            {
                if (0 == StringComparer.OrdinalIgnoreCase.Compare(item.Name, name))
                {
                    return item;
                }
            }

            return null;
        }

        /// <summary>
        /// For some scenarios, it is more convenient to extract identifier values from a single object.
        /// This reduces the number of arguments passed into the lambda, improving execution time.
        /// Context has predefined name "Context", and is used by "Identifier" atoms to invoke methods, read fields etc.
        /// </summary>
        public ParameterExpression Context
        {
            get { return TryGetArgumentByName("@Context"); }
        }

        /// <summary>
        /// For some scenarios, it is more convenient to extract identifier values from a single object.
        /// This reduces the number of arguments passed into the lambda, improving execution time.
        /// Context has predefined name "Context", and is used by "Identifier" atoms to invoke methods, read fields etc.
        /// </summary>
        /// <exception cref="CompilationException">Context argument is not registered</exception>
        public ParameterExpression RequireContext()
        {
            var result = Context;
            if (result == null)
            {
                throw new CompilationException("Argument @Context is not registered");
            }

            return result;
        }
    }
}