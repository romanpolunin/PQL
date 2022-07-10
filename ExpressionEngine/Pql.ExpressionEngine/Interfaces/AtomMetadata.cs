using System.Linq.Expressions;
using System.Reflection;

using Irony.Parsing;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// A placeholder to register extensions to runtime.
    /// </summary>
    /// <seealso cref="IExpressionEvaluatorRuntime.IsAtomRegistered"/>
    /// <seealso cref="IExpressionEvaluatorRuntime.RegisterAtom"/>
    public class AtomMetadata
    {
        /// <summary>
        /// Type of this atom.
        /// </summary>
        public readonly AtomType AtomType;

        /// <summary>
        /// Delegate type for use by <see cref="AtomMetadata.ExpressionGenerator"/>.
        /// </summary>
        /// <param name="root">Parse tree node, for which an expression must be generated</param>
        /// <param name="state">Current state of the compiler instance, has context values</param>
        /// <seealso cref="AtomMetadata.ExpressionGenerator"/>
        /// <seealso cref="CompilerState"/>
        public delegate Expression? ExpressionGeneratorCallback(ParseTreeNode root, CompilerState state);

        /// <summary>
        /// Name of the element, will be used to parse expression.
        /// May contain dots, dotted notation will by default be transformed into field/property access expressions.
        /// Do not use special character '@' in atom names, to avoid conflicts with arguments used by expression.
        /// </summary>
        public readonly string Name;

        /// <summary>
        /// An alternative to <see cref="Functor"/>, client can supply an expression generator delegate.
        /// This delegate will be invoked during compilation time, in order to create an Expression object given the parse tree context.
        /// </summary>
        /// <seealso cref="Functor"/>
        /// <seealso cref="MethodInfo"/>
        public readonly ExpressionGeneratorCallback? ExpressionGenerator;

        /// <summary>
        /// Must be some sort of Func{TArg1, TArg2, ..., TResult}, where types must be compatible with logical data types.
        /// May have zero or more arguments, may constitute a field accessor or function.
        /// Compilation engine will bind arguments, if any, to real arguments of this functor.
        /// </summary>
        /// <seealso cref="MethodInfo"/>
        /// <seealso cref="ExpressionGenerator"/>
        public readonly object? Functor;

        /// <summary>
        /// Reflected method information on the functor. Automatically derived from the functor's reflected metadata.
        /// Can be null if <see cref="Functor"/> or <see cref="ExpressionGenerator"/> are not null.
        /// </summary>
        /// <seealso cref="Functor"/>
        /// <seealso cref="ExpressionGenerator"/>
        public readonly MethodInfo? MethodInfo;

        /// <summary>
        /// Closure instance on the functor. Automatically derived from the functor's reflected metadata.
        /// Can be null if <see cref="MethodInfo"/> is null, or is for a static method.
        /// </summary>
        /// <seealso cref="Functor"/>
        /// <seealso cref="ExpressionGenerator"/>
        public readonly object? MethodTarget;

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="atomType">Atom type.</param>
        /// <param name="name">The name to be used for parsing. See <see cref="Name"/>.</param>
        /// <param name="functor">Functor responsible for extracting values from source. See <see cref="Functor"/>.</param>
        public AtomMetadata(AtomType atomType, string name, object functor)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            ValidateFunctor(functor);

            AtomType = atomType;
            Name = name;
            Functor = functor;
            MethodInfo = TryGetMethodInfo(functor);
            MethodTarget = TryGetMethodTarget(functor);
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        /// <param name="atomType">Atom type.</param>
        /// <param name="name">The name to be used for parsing. See <see cref="Name"/>.</param>
        /// <param name="expressionGenerator">The functor responsible for generating expressions.</param>
        public AtomMetadata(AtomType atomType, string name, ExpressionGeneratorCallback expressionGenerator)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (expressionGenerator == null)
            {
                throw new ArgumentNullException(nameof(expressionGenerator));
            }

            AtomType = atomType;
            Name = name;
            ExpressionGenerator = expressionGenerator;
        }

        private MethodInfo? TryGetMethodInfo(object functor)
        {
            var prop = functor.GetType().GetProperty("Method");
            var result = prop?.GetValue(functor, null) as MethodInfo;
            return result;
        }

        private object? TryGetMethodTarget(object functor)
        {
            var prop = functor.GetType().GetProperty("Target");
            var result = prop?.GetValue(functor, null);
            return result;
        }

        private void ValidateFunctor(object functor)
        {
            if (functor == null)
            {
                throw new ArgumentException("Invalid atom: null functor");
            }

            var type = functor.GetType();
            if (type.IsConstructedGenericType && type.Name.StartsWith("Func"))
            {
                return;
            }

            throw new ArgumentException("Invalid functor type: " + type.FullName);
        }
    }

    /// <summary>
    /// Possible values for atom types.
    /// </summary>
    public enum AtomType
    {
        /// <summary>
        /// Dummy value to help detect bugs.
        /// </summary>
        InvalidValue = default,
        /// <summary>
        /// Identifier (non-function, non-array).
        /// </summary>
        Identifier,
        /// <summary>
        /// Function call, may have zero or more arguments.
        /// </summary>
        Function
    }
}