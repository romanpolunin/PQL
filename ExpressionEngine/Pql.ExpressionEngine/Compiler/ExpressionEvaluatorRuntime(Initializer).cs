using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

using Irony.Parsing;

using Pql.ExpressionEngine.Grammar;
using Pql.ExpressionEngine.Interfaces;
using Pql.ExpressionEngine.Utilities;


#pragma warning disable IDE0049
namespace Pql.ExpressionEngine.Compiler
{
    public partial class ExpressionEvaluatorRuntime
    {
        private static readonly LanguageData s_langData;
        private static readonly NonTerminal s_expressionNonTerminal;

        private readonly ObjectPool<Parser> _expressionParsers;
        private readonly ConcurrentDictionary<string, AtomMetadata> _atoms;
        private readonly ConcurrentBag<AtomMetadata> _atomHandlers;

        static ExpressionEvaluatorRuntime()
        {
            Irony.Parsing.Grammar grammar = new ExpressionGrammar();
            s_langData = new LanguageData(grammar);
            s_expressionNonTerminal = s_langData.GrammarData.NonTerminals.Single(x => x.Name == "expression");
        }

        /// <summary>
        /// Ctr.
        /// </summary>
        public ExpressionEvaluatorRuntime()
        {
            var maxDegreeOfParallelism = Environment.ProcessorCount * 4;
            _atoms = new ConcurrentDictionary<string, AtomMetadata>(StringComparer.OrdinalIgnoreCase);
            _atomHandlers = new ConcurrentBag<AtomMetadata>();

            _expressionParsers = new ObjectPool<Parser>(maxDegreeOfParallelism, () => new Parser(s_langData, s_expressionNonTerminal));

            RegisterAtom(new AtomMetadata(AtomType.Identifier, "Null", PredefinedAtom_VoidNull));
            RegisterAtom(new AtomMetadata(AtomType.Function, "IsNull", PredefinedAtom_IsNull));
            RegisterAtom(new AtomMetadata(AtomType.Function, "IfNull", PredefinedAtom_IfNull));
            RegisterAtom(new AtomMetadata(AtomType.Identifier, "false", PredefinedAtom_False));
            RegisterAtom(new AtomMetadata(AtomType.Identifier, "true", PredefinedAtom_True));
            RegisterAtom(new AtomMetadata(AtomType.Function, "IsDefault", PredefinedAtom_IsDefault));
            RegisterAtom(new AtomMetadata(AtomType.Function, "Default", PredefinedAtom_Default));
            RegisterAtom(new AtomMetadata(AtomType.Function, "Coalesce", PredefinedAtom_Coalesce));
            RegisterAtom(new AtomMetadata(AtomType.Identifier, "PositiveInfinity", PredefinedAtom_PositiveInfinity));
            RegisterAtom(new AtomMetadata(AtomType.Identifier, "NegativeInfinity", PredefinedAtom_NegativeInfinity));
            RegisterAtom(new AtomMetadata(AtomType.Identifier, "NaN", PredefinedAtom_NaN));
            RegisterAtom(new AtomMetadata(AtomType.Function, "IsNaN", PredefinedAtom_IsNaN));
            RegisterAtom(new AtomMetadata(AtomType.Function, "IsInfinity", PredefinedAtom_IsInfinity));
            RegisterAtom(new AtomMetadata(AtomType.Function, "EndsWith", PredefinedAtom_EndsWith));
            RegisterAtom(new AtomMetadata(AtomType.Function, "StartsWith", PredefinedAtom_StartsWith));
            RegisterAtom(new AtomMetadata(AtomType.Function, "Contains", PredefinedAtom_Contains));
            RegisterAtom(new AtomMetadata(AtomType.Function, "Convert", PredefinedAtom_Convert));
            RegisterAtom(new AtomMetadata(AtomType.Function, "Cast", PredefinedAtom_Cast));
            RegisterAtom(new AtomMetadata(AtomType.Function, "ToDateTime", PredefinedAtom_ToDateTime));
            RegisterAtom(new AtomMetadata(AtomType.Function, "NewGUID", PredefinedAtom_NewGUID));
            RegisterAtom(new AtomMetadata(AtomType.Function, "SetContains", PredefinedAtom_SetContains));
        }

        private Expression PredefinedAtom_SetContains(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(2);

            var arg1Node = root.RequireChild(null, 1, 0, 0);
            var hashset = state.ParentRuntime.Analyze(arg1Node, state);
            hashset.RequireNonVoid(arg1Node);
            if (hashset.Type.IsValueType)
            {
                throw new CompilationException("Set must be of reference type", arg1Node);
            }

            if (hashset is ConstantExpression)
            {
                throw new CompilationException("Set must not be a constant expression", arg1Node);
            }

            var arg2Node = root.RequireChild(null, 1, 0, 1);
            var element = state.ParentRuntime.Analyze(arg2Node, state);

            var methods = hashset.Type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.InvokeMethod | BindingFlags.FlattenHierarchy);
            foreach (var method in methods)
            {
                if (method.Name.Equals("Contains"))
                {
                    var methodArgs = method.GetParameters();
                    if (methodArgs.Length == 1 && methodArgs[0].ParameterType != typeof(object))
                    {
                        if (ExpressionTreeExtensions.TryAdjustReturnType(arg2Node, element, methodArgs[0].ParameterType, out var adjusted))
                        {
                            return Expression.Condition(
                                Expression.ReferenceEqual(hashset, Expression.Constant(null)),
                                Expression.Constant(false, typeof(bool)),
                                Expression.Call(hashset, method, adjusted));
                        }
                    }
                }
            }

            throw new CompilationException(
                "Could not find a public instance method 'Contains' to match element type " + element.Type.FullName, arg1Node);
        }

        private Expression PredefinedAtom_IfNull(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(2);

            var arg1Node = root.RequireChild(null, 1, 0, 0);
            var argument = state.ParentRuntime.Analyze(arg1Node, state);
            var isnullableType = argument.IsNullableType();

            var arg2Node = root.RequireChild(null, 1, 0, 1);
            var ifnull = state.ParentRuntime.Analyze(arg2Node, state);

            if (argument.IsVoid())
            {
                return ifnull;
            }

            if (!ExpressionTreeExtensions.TryAdjustVoid(ref argument, ref ifnull))
            {
                return PredefinedAtom_VoidNull(root, state);
            }

            ifnull = ExpressionTreeExtensions.AdjustReturnType(arg2Node, ifnull, argument.Type.RequireUnderlyingType());

            Expression result;
            if (argument.Type.IsValueType)
            {
                result = isnullableType
                    ? Expression.Condition(Expression.Field(argument, "HasValue"), Expression.Field(argument, "Value"), ifnull)
                    : argument;
            }
            else
            {
                // now only reference types remaining
                if (argument is ConstantExpression constantExpression)
                {
                    result = constantExpression.Value is null ? ifnull : argument;
                }
                else
                {
                    result = Expression.Condition(Expression.Equal(argument, Expression.Constant(null)), ifnull, argument);
                }
            }

            return result.IsVoid() ? result : result.RemoveNullability();
        }

        private Expression PredefinedAtom_IsNull(ParseTreeNode root, CompilerState state)
        {
            var funArgs = ExpressionTreeExtensions.UnwindTupleExprList(root.RequireChild("exprList", 1, 0));
            funArgs.RequireChildren(1);

            var arg1Node = funArgs.ChildNodes[0];
            return BuildIsNullPredicate(arg1Node, state, true);
        }

        private static Expression PredefinedAtom_NewGUID(ParseTreeNode root, CompilerState state) => 
            Expression.Call(typeof(Guid), "NewGuid", null);

        private static Expression PredefinedAtom_VoidNull(ParseTreeNode root, CompilerState state) =>
            // clients are responsible for translating this expression into appropriate type
            ExpressionTreeExtensions.MakeNewNullable(typeof(UnboxableNullable<ExpressionTreeExtensions.VoidTypeMarker>));

        private static Expression PredefinedAtom_False(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(1);
            return Expression.Constant(false);
        }

        private static Expression PredefinedAtom_True(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(1);
            return Expression.Constant(true);
        }


        /// <summary>
        /// Returns first argument if it's not null and not equal to default value for that type.
        /// Returns second argument otherwise.
        /// Type of second argument must be adjustable to the type of first argument.
        /// </summary>
        public static Expression PredefinedAtom_Coalesce(ParseTreeNode root, CompilerState state)
        {
            // cannot supply more than twenty alternative options, must supply at least one
            var funArgs = ExpressionTreeExtensions.UnwindTupleExprList(root.RequireChild("exprList", 1, 0));
            funArgs.RequireChildren(2, 21);

            var lastNode = funArgs.RequireChild(null, funArgs.ChildNodes.Count - 1);
            var lastValue = state.ParentRuntime.Analyze(lastNode, state);
            lastValue.RequireNonVoid(lastNode);

            var result = lastValue;
            for (var i = funArgs.ChildNodes.Count - 2; i >= 0; i--)
            {
                var thisNode = funArgs.RequireChild(null, i);
                var thisValue = state.ParentRuntime.Analyze(thisNode, state);
                thisValue.RequireNonVoid(thisNode);

                var isDefault = IsDefault(thisNode, thisValue);
                if (thisValue.IsNullableType() && !result.IsNullableType())
                {
                    result = ExpressionTreeExtensions.MakeNewNullable(result);
                }
                else if (!thisValue.IsNullableType() && result.IsNullableType())
                {
                    thisValue = ExpressionTreeExtensions.MakeNewNullable(thisValue);
                }

                result = Expression.Condition(isDefault, result, thisValue);
            }

            return result;
        }

        private static Expression IsDefault(ParseTreeNode root, Expression value)
        {
            if (value.IsString())
            {
                if (value is ConstantExpression constValue)
                {
                    return Expression.Constant(string.IsNullOrEmpty((string?)constValue.Value), typeof(Boolean));
                }

                return Expression.Call(ReflectionHelper.StringIsNullOrEmpty, value);
            }

            return ConstantHelper.TryEvalConst(root, value, Expression.Default(value.Type), ExpressionType.Equal);
        }

        private static Expression PredefinedAtom_IsDefault(ParseTreeNode root, CompilerState state)
        {
            var funArgs = ExpressionTreeExtensions.UnwindTupleExprList(root.RequireChild("exprList", 1, 0));
            funArgs.RequireChildren(1);

            var arg1Node = funArgs.ChildNodes[0];
            var value = state.ParentRuntime.Analyze(arg1Node, state).RemoveNullability();

            if (value.IsString())
            {
                if (value is ConstantExpression constValue)
                {
                    return Expression.Constant(string.IsNullOrEmpty((string?)constValue.Value), typeof(Boolean));
                }

                return Expression.Call(ReflectionHelper.StringIsNullOrEmpty, value);
            }

            return ConstantHelper.TryEvalConst(root, value, Expression.Default(value.Type), ExpressionType.Equal);
        }

        private static Expression PredefinedAtom_IsNaN(ParseTreeNode root, CompilerState state)
        {
            var funArgs = ExpressionTreeExtensions.UnwindTupleExprList(root.RequireChild("exprList", 1, 0));
            funArgs.RequireChildren(1);

            var arg1Node = funArgs.ChildNodes[0];
            var value = state.ParentRuntime.Analyze(arg1Node, state).RemoveNullability();

            var constExpr = value as ConstantExpression;

            if (ReferenceEquals(value.Type, typeof(Double)))
            {
                return constExpr != null
                           ? Expression.Constant(Double.IsNaN((Double)(constExpr.Value ?? 0.0)))
                           : Expression.Call(ReflectionHelper.DoubleIsNaN, value);
            }

            if (ReferenceEquals(value.Type, typeof(Single)))
            {
                return constExpr != null
                           ? Expression.Constant(Single.IsNaN((Single)(constExpr.Value ?? 0f)))
                           : Expression.Call(ReflectionHelper.SingleIsNaN, value);
            }

            throw new CompilationException("IsNaN requires argument of type Single or Double. Actual: " + value.Type.FullName, arg1Node);
        }

        private static Expression PredefinedAtom_IsInfinity(ParseTreeNode root, CompilerState state)
        {
            var funArgs = ExpressionTreeExtensions.UnwindTupleExprList(root.RequireChild("exprList", 1, 0));
            funArgs.RequireChildren(1);

            var arg1Node = funArgs.ChildNodes[0];
            var value = state.ParentRuntime.Analyze(arg1Node, state).RemoveNullability();

            var constExpr = value as ConstantExpression;

            if (ReferenceEquals(value.Type, typeof(Double)))
            {
                return constExpr != null
                           ? Expression.Constant(Double.IsInfinity((Double)(constExpr.Value ?? 0.0)))
                           : Expression.Call(ReflectionHelper.DoubleIsInfinity, value);
            }

            if (ReferenceEquals(value.Type, typeof(Single)))
            {
                return constExpr != null
                           ? Expression.Constant(Single.IsInfinity((Single)(constExpr.Value ?? 0f)))
                           : Expression.Call(ReflectionHelper.SingleIsInfinity, value);
            }

            throw new CompilationException("IsInfinity requires argument of type Single or Double. Actual: " + value.Type.FullName, arg1Node);
        }

        private static Expression PredefinedAtom_Default(ParseTreeNode root, CompilerState state)
        {
            var funArgs = ExpressionTreeExtensions.UnwindTupleExprList(root.RequireChild("exprList", 1, 0));
            funArgs.RequireChildren(1);

            var arg1Node = funArgs.ChildNodes[0];
            var targetType = RequireSystemType(arg1Node);

            return ExpressionTreeExtensions.GetDefaultExpression(targetType);
        }

        private static Expression PredefinedAtom_NegativeInfinity(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(1);
            return Expression.Constant(Double.NegativeInfinity);
        }

        private static Expression PredefinedAtom_NaN(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(1);
            return Expression.Constant(Double.NaN);
        }

        private static Expression PredefinedAtom_PositiveInfinity(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(1);
            return Expression.Constant(Double.PositiveInfinity);
        }

        private static Expression PredefinedAtom_EndsWith(ParseTreeNode root, CompilerState state) => PredefinedAtom_StringLike(root, "EndsWith", state);

        private static Expression PredefinedAtom_StartsWith(ParseTreeNode root, CompilerState state) => PredefinedAtom_StringLike(root, "StartsWith", state);

        private static Expression PredefinedAtom_Contains(ParseTreeNode root, CompilerState state) => PredefinedAtom_StringLike(root, "IndexOf", state);

        private static Expression PredefinedAtom_StringLike(ParseTreeNode root, string methodName, CompilerState state)
        {
            var method = PrepareStringInstanceMethodCall(methodName, ExpressionTreeExtensions.UnwindTupleExprList(root), state, out var value, out var pattern);

            var constValue = value as ConstantExpression;
            var constPattern = pattern as ConstantExpression;

            if (constValue is not null && constPattern is not null)
            {
                return constValue.Value == null || constPattern.Value == null
                    ? Expression.Constant(false)
                    : ConstantHelper.TryEvalConst(root, method, constValue, constPattern, Expression.Constant(StringComparison.OrdinalIgnoreCase));
            }

            Expression target = Expression.Call(value, method, pattern, Expression.Constant(StringComparison.OrdinalIgnoreCase));
            if (target.Type == typeof(Int32))
            {
                target = Expression.GreaterThanOrEqual(target, Expression.Constant(0, target.Type));
            }

            return Expression.Condition(
                Expression.ReferenceEqual(Expression.Constant(null), value), Expression.Constant(false),
                Expression.Condition(
                    Expression.ReferenceEqual(Expression.Constant(null), pattern), Expression.Constant(false),
                    target));
        }

        private static Expression PredefinedAtom_Convert(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(2);

            var arg1Node = root.RequireChild(null, 1, 0, 0);
            var value = state.ParentRuntime.Analyze(arg1Node, state).RemoveNullability();

            var arg2Node = root.RequireChild("string", 1, 0, 1);
            var targetType = RequireSystemType(arg2Node);

            return PredefinedAtom_Convert_SingleValue(root, value, arg2Node, targetType);
        }

        private static Expression PredefinedAtom_Convert_SingleValue(ParseTreeNode? root, Expression value, ParseTreeNode arg2Node, Type targetType)
        {
            // maybe we don't have to change type, or simply cast the numeric type?
            if (ExpressionTreeExtensions.TryAdjustReturnType(root, value, targetType, out var adjusted))
            {
                return adjusted;
            }

            if (value.IsString())
            {
                // or maybe they are asking to convert Base64 string into binary value?
                if (targetType.IsBinary())
                {
                    var ctr = targetType.GetConstructor(new[] { typeof(string) })
                        ?? throw new CompilationException("Could not find a constructor that takes string as argument, on type " + targetType.FullName);
                    return ConstantHelper.TryEvalConst(root, ctr, value);
                }

                // maybe we can parse string to a number?
                if (targetType.IsNumeric() || targetType.IsDateTime() || targetType.IsTimeSpan() || targetType.IsGuid())
                {
                    var parseMethod = ReflectionHelper.GetOrAddMethod1(targetType, "Parse", value.Type);
                    return Expression.Condition(
                        ConstantHelper.TryEvalConst(root, ReflectionHelper.StringIsNullOrEmpty, value),
                        Expression.Default(targetType),
                        ConstantHelper.TryEvalConst(root, parseMethod, value)
                        );
                }
            }

            // maybe we can generate a string from a number or other object?
            if (targetType.IsString())
            {
                if (value.IsBinary())
                {
                    var toStringMethod = ReflectionHelper.GetOrAddMethod1(value.Type, "ToBase64String", value.Type);
                    return ConstantHelper.TryEvalConst(root, toStringMethod, value);
                }
                else
                {
                    var toStringMethod = ReflectionHelper.GetOrAddMethod0(value.Type, "ToString");
                    return ConstantHelper.TryEvalConst(root, toStringMethod, value);
                }

            }

            // seems like cast does not apply, let's use converter
            try
            {
                var convertMethod = ReflectionHelper.GetOrAddMethod1(typeof(Convert), "To" + targetType.Name, value.Type);
                return ConstantHelper.TryEvalConst(root, convertMethod, value);
            }
            catch
            {
                throw new CompilationException($"There is no conversion from type {value.Type.FullName} to type {targetType.FullName}", arg2Node);
            }
        }

        private static Expression PredefinedAtom_Cast(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(2);

            var arg1Node = root.RequireChild(null, 1, 0, 0);
            var value = state.ParentRuntime.Analyze(arg1Node, state).RemoveNullability();

            var arg2Node = root.RequireChild("string", 1, 0, 1);
            var targetType = RequireSystemType(arg2Node);

            if (value.IsVoid())
            {
                return ExpressionTreeExtensions.GetDefaultExpression(targetType);
            }

            // bluntly attempt to convert the type; will throw if types are not compatible
            return ConstantHelper.TryEvalConst(root, value, ExpressionType.Convert, targetType);
        }

        private static Expression PredefinedAtom_ToDateTime(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(2);
            var argNodes = root.RequireChild(null, 1, 0);

            if (argNodes.ChildNodes.Count is 3 or 6 or 7)
            {
                var args = new Expression[argNodes.ChildNodes.Count];
                var typeArray = new Type[args.Length];
                var typeInt32 = typeof(Int32);
                for (var i = 0; i < args.Length; i++)
                {
                    var node = argNodes.RequireChild(null, i);
                    var expr = ExpressionTreeExtensions.AdjustReturnType(node, state.ParentRuntime.Analyze(node, state), typeInt32);
                    args[i] = expr;
                    typeArray[i] = typeInt32;
                }

                var ctr = typeof(DateTime).GetConstructor(typeArray);
                if (ctr == null)
                {
                    throw new Exception($"Could not locate datetime constructor with {typeArray.Length} int arguments");
                }

                return ConstantHelper.TryEvalConst(root, ctr, args);
            }

            if (argNodes.ChildNodes.Count == 2)
            {
                var arg1Node = root.RequireChild(null, 1, 0, 0);
                var valueString = state.ParentRuntime.Analyze(arg1Node, state).RemoveNullability();
                valueString.RequireString(arg1Node);

                var arg2Node = root.RequireChild(null, 1, 0, 1);
                var formatString = state.ParentRuntime.Analyze(arg2Node, state).RemoveNullability();
                formatString.RequireString(arg2Node);

                return ConstantHelper.TryEvalConst(root, ReflectionHelper.DateTimeParseExact,
                    valueString, formatString, Expression.Constant(null, typeof(IFormatProvider)));
            }

            if (argNodes.ChildNodes.Count == 1)
            {
                var arg1Node = root.RequireChild(null, 1, 0, 0);
                var value = state.ParentRuntime.Analyze(arg1Node, state).RemoveNullability();
                value.RequireInteger(arg1Node);

                var dateTimeFromBinary = ReflectionHelper.GetOrAddMethod1(typeof(DateTime), "FromBinary", typeof(Int64));
                return ConstantHelper.TryEvalConst(root, dateTimeFromBinary, value);
            }

            throw new CompilationException("ToDateTime has four overloads: with two, three, six and seven arguments", root);
        }

        private static Type RequireSystemType(ParseTreeNode argNode)
        {
            Type? targetType;
            try
            {
                var targetTypeName = argNode.Token.ValueString;
                if (0 == StringComparer.OrdinalIgnoreCase.Compare(targetTypeName, "Binary"))
                {
                    return typeof(SizableArrayOfByte);
                }

                targetType = Type.GetType("System." + targetTypeName, false, true);
            }
            catch (Exception e)
            {
                throw new CompilationException("Could not find target type by name because of error: " + e.Message, argNode);
            }

            if (targetType == null)
            {
                throw new CompilationException("Could not find target type by name: " + argNode, argNode);
            }

            return targetType;
        }
    }
}
