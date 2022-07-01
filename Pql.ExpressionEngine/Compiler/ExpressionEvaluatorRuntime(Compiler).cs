using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

using Irony.Parsing;

using Pql.ExpressionEngine.Interfaces;

namespace Pql.ExpressionEngine.Compiler
{
    public partial class ExpressionEvaluatorRuntime
    {
        private static MethodInfo PrepareStringInstanceMethodCall(string methodName, ParseTreeNode root, CompilerState state, out Expression value, out Expression pattern)
        {
            root.RequireChildren(2);

            MethodInfo method;
            if (0 == StringComparer.OrdinalIgnoreCase.Compare(methodName, "StartsWith"))
            {
                method = ReflectionHelper.StringStartsWith;
            }
            else if (0 == StringComparer.OrdinalIgnoreCase.Compare(methodName, "EndsWith"))
            {
                method = ReflectionHelper.StringEndsWith;
            }
            else if (0 == StringComparer.OrdinalIgnoreCase.Compare(methodName, "IndexOf"))
            {
                method = ReflectionHelper.StringIndexOf;
            }
            else
            {
                throw new Exception("Could not find method " + methodName);
            }

            var arg1Node = root.RequireChild(null, 1, 0, 0);
            value = state.ParentRuntime.Analyze(arg1Node, state);
            value.RequireString(arg1Node);

            var arg2Node = root.RequireChild(null, 1, 0, 1);
            pattern = state.ParentRuntime.Analyze(arg2Node, state);
            pattern.RequireString(arg2Node);
            return method;
        }

        private string BuildParserErrorMessage(ParseTree result)
        {
            var builder = new StringBuilder(2000);
            builder.AppendLine("Failed to parse expression. See parser output below.");
            foreach (var msg in result.ParserMessages)
            {
                builder.AppendFormat("{0} - {1} at {2}: {3}", msg.ParserState, msg.Level,
                    CompilationException.FormatLocationString(msg.Location, -1), msg.Message);
                builder.AppendLine();
            }

            return builder.ToString();
        }

        private Expression Analyze(ParseTree tree, CompilerState state)
        {
            if (tree == null)
            {
                throw new ArgumentNullException(nameof(tree));
            }

            if (tree.Status != ParseTreeStatus.Parsed)
            {
                throw new ArgumentException("Cannot build expression on incomplete tree");
            }

            var root = tree.Root;
            return Analyze(root, state);
        }

        private Expression BuildStringLiteralExpression(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(0);
            var value = root.Token.ValueString;
            var expr = string.IsNullOrEmpty(value)
                ? Expression.Constant(null, typeof(String)) : Expression.Constant(PreprocessStringLiteral(value, root));
            return expr;
        }

        private string PreprocessStringLiteral(string value, ParseTreeNode root)
        {
            try
            {
                // token string may contain special characters like newlines, tabs etc. 
                // let's handle them by passing the value through string.Format
                return string.Format(value);
            }
            catch (Exception e)
            {
                throw new CompilationException("Failed to preprocess string literal: " + e.Message, root);
            }
        }

        private Expression BuildNumericConstantExpression(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(0);

            var objValue = root.Token.Value;
            if (objValue != null && objValue.GetType().IsNumeric())
            {
                return Expression.Constant(objValue);
            }

            throw new CompilationException("Invalid numeric constant: " + root.Token.Text, root);
        }

        private Expression BuildIdentifierExpression(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(1, 10);

            // first, analyze the first identifier in the dotted sequence
            var identifier = BuildIdentifierRootExpression(root, state);

            // next, look for any next-level dotted fields and properties
            // for these, we will not look into any other atoms or args - only into the reflected fields and properties of the object
            for (var i = 1; i < root.ChildNodes.Count; i++)
            {
                var name = root.ChildNodes[i].Token.Text; // no need to lowercase this text
                var next = TryGetFieldOrPropertyInfoFromContext(name, identifier);
                identifier = next ?? throw new CompilationException(string.Format("Could not find field or property {0} on type {1}",
                        name, identifier.Type));
            }

            return identifier;
        }

        private Expression BuildIdentifierRootExpression(ParseTreeNode root, CompilerState state)
        {
            var name = root.ChildNodes[0].Token.ValueString;

            // first, look for an argument with this name
            var argument = state.TryGetArgumentByName(name);
            if (argument != null)
            {
                return argument;
            }

            var context = state.Context;

            // next, see if we have a field or property on the context (if any context present)
            var contextBoundExpression = TryGetFieldOrPropertyInfoFromContext(name, context);
            if (contextBoundExpression != null)
            {
                return contextBoundExpression;
            }

            // and only then look through available IDENTIFIER atoms
            if (m_atoms.TryGetValue(name, out var atom) && atom.AtomType == AtomType.Identifier)
            {
                if (atom.ExpressionGenerator != null)
                {
                    return atom.ExpressionGenerator(root, state);
                }

                if (atom.MethodInfo == null)
                {
                    // internal error, somebody screwed up with configuration of runtime
                    throw new Exception("ExpressionGenerator and MethodInfo are both null on atom: " + atom.Name);
                }

                // no arguments? great
                var paramInfo = atom.MethodInfo.GetParameters();
                if (paramInfo.Length == 0)
                {
                    return BuildFunctorInvokeExpression(atom, (Expression[])null);
                }

                // any arguments? must have exactly one argument, context must be registered, and context type must be adjustable to this method's arg type
                if (context == null)
                {
                    throw new CompilationException("Atom's MethodInfo cannot be used for an Id expression, because context is not available: " + atom.Name, root);
                }

                if (paramInfo.Length > 1 || !ExpressionTreeExtensions.TryAdjustReturnType(root, context, paramInfo[0].ParameterType, out var adjustedContext))
                {
                    throw new CompilationException("Atom's MethodInfo may only have either zero arguments or one argument of the same type as expression context: " + atom.Name, root);
                }

                return BuildFunctorInvokeExpression(atom, adjustedContext);
            }

            // still nothing found? ask IDENTIFIER atom handlers
            foreach (var handler in m_atomHandlers)
            {
                if (handler.AtomType != AtomType.Identifier)
                {
                    continue;
                }

                if (handler.ExpressionGenerator == null)
                {
                    // internal error, somebody screwed up with configuration of runtime
                    throw new Exception("ExpressionGenerator is null on atom handler: " + handler.Name);
                }

                // only pass the first portion of dot-notation identifier to handler
                var result = handler.ExpressionGenerator(root.ChildNodes[0], state);
                if (result != null)
                {
                    return result;
                }
            }

            throw new CompilationException("Unknown atom: " + name, root);
        }

        private Expression BuildFunctorInvokeExpression(AtomMetadata atom, Expression[] args)
        {
            return atom.MethodTarget == null
                    ? Expression.Call(atom.MethodInfo, args)
                    : Expression.Call(Expression.Constant(atom.MethodTarget), atom.MethodInfo, args);
        }

        private Expression BuildFunctorInvokeExpression(AtomMetadata atom, Expression adjustedContext)
        {
            if (adjustedContext == null)
            {
                return atom.MethodTarget == null
                        ? Expression.Call(atom.MethodInfo)
                        : Expression.Call(Expression.Constant(atom.MethodTarget), atom.MethodInfo);
            }

            return atom.MethodTarget == null
                    ? Expression.Call(atom.MethodInfo, adjustedContext)
                    : Expression.Call(Expression.Constant(atom.MethodTarget), atom.MethodInfo, adjustedContext);
        }

        private Expression TryGetFieldOrPropertyInfoFromContext(string name, Expression context)
        {
            if (context == null)
            {
                return null;
            }

            var property = context.Type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (property != null)
            {
                return Expression.Property(context, property);
            }

            var field = context.Type.GetField(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (field != null)
            {
                return Expression.Field(context, field);
            }

            return null;
        }

        private Expression BuildFunCallExpression(ParseTreeNode root, CompilerState state)
        {
            // funCall -> Id
            var idNode = root.RequireChild("Id", 0);
            idNode.RequireChildren(1); // dotted identifiers are not supported for function calls

            // funCall -> funArgs 
            root.RequireChild("funArgs", 1);

            var name = idNode.ChildNodes[0].Token.ValueString;
            if (!m_atoms.TryGetValue(name, out var atom) || atom.AtomType != AtomType.Function)
            {
                throw new CompilationException("Unknown function: " + name, root);
            }

            if (atom.MethodInfo != null)
            {
                return BuildFunCallExpressionFromMethodInfo(atom, root, state);
            }

            if (atom.ExpressionGenerator != null)
            {
                return atom.ExpressionGenerator(root, state);
            }

            // internal error, somebody screwed up with configuration of runtime
            throw new Exception("ExpressionGenerator and MethodInfo are both null on atom: " + atom.Name);
        }

        private Expression BuildFunCallExpressionFromMethodInfo(AtomMetadata atom, ParseTreeNode root, CompilerState state)
        {
            // number of arguments must exactly match number of child nodes in the tree
            var paramInfo = atom.MethodInfo.GetParameters();

            var funArgs = root.RequireChild("exprList", 1, 0);
            funArgs.RequireChildren(paramInfo.Length);

            // types and order of arguments must match nodes in the tree
            var args = new Expression[paramInfo.Length];

            for (var i = 0; i < paramInfo.Length; i++)
            {
                var param = paramInfo[i];
                var argNode = funArgs.ChildNodes[i];
                var value = state.ParentRuntime.Analyze(argNode, state);

                if (!ExpressionTreeExtensions.TryAdjustReturnType(root, value, param.ParameterType, out var adjusted))
                {
                    throw new CompilationException(string.Format("Could not adjust parameter number {0} to invoke function {1}",
                        i, atom.Name), funArgs.ChildNodes[i]);
                }

                args[i] = adjusted;
            }

            return BuildFunctorInvokeExpression(atom, args);
        }

        private Expression BuildCaseStatementExpression(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(4, 5);
            root.RequireChild("CASE", 0);
            var caseVariableNode = root.RequireChild("caseVariable", 1);
            caseVariableNode.RequireChildren(0, 1);

            ParseTreeNode caseDefaultValueNode = null;
            if (root.ChildNodes.Count > 4)
            {
                var caseDefaultNode = root.RequireChild("caseDefault", 3);
                if (caseDefaultNode.ChildNodes.Count > 0)
                {
                    caseDefaultNode.RequireChildren(2);
                    caseDefaultNode.RequireChild("else", 0);
                    caseDefaultValueNode = caseDefaultNode.RequireChild(null, 1);
                }
            }

            root.RequireChild("END", root.ChildNodes.Count - 1);

            // first get the expression for ELSE
            var caseDefault = caseDefaultValueNode != null ? Analyze(caseDefaultValueNode, state) : null;

            var whenThenListNode = root.RequireChild("caseWhenList", 2);
            whenThenListNode.RequireChildren(1, 100);

            // now if we don't have an expression to be analyzed, just create a sequence of IIFs
            if (caseVariableNode.ChildNodes.Count == 0)
            {
                return BuildCaseStatementExpression(state, whenThenListNode, caseDefault);
            }

            // but when there is a variable, type checking has different rules and output is SWITCH statement
            return BuildSwitchStatementExpression(state, caseVariableNode, whenThenListNode, caseDefault);
        }

        private Expression BuildSwitchStatementExpression(CompilerState state, ParseTreeNode caseVariableNode, ParseTreeNode whenThenListNode, Expression caseDefault)
        {
            var switchVariable = Analyze(caseVariableNode.ChildNodes[0], state);
            switchVariable.RequireNonVoid(caseVariableNode.ChildNodes[0]);

            if (switchVariable is ConstantExpression)
            {
                throw new CompilationException("CASE variable should not be a constant value", caseVariableNode);
            }

            var cases = new List<Tuple<Expression[], Expression, ParseTreeNode>>(whenThenListNode.ChildNodes.Count);
            Expression firstNonVoidThen = null;
            var mustReturnNullable = false;

            foreach (var caseWhenThenNode in whenThenListNode.ChildNodes)
            {
                caseWhenThenNode.RequireChildren(4);
                var whenNodesRoot = ExpressionTreeExtensions.UnwindTupleExprList(caseWhenThenNode.RequireChild(null, 1));
                var thenNode = caseWhenThenNode.RequireChild(null, 3);

                IList<ParseTreeNode> whenNodes;
                if (whenNodesRoot.Term.Name == "exprList")
                {
                    whenNodes = whenNodesRoot.ChildNodes;
                }
                else
                {
                    whenNodes = new[] { whenNodesRoot };
                }

                var when = new Expression[whenNodes.Count];
                for (var i = 0; i < whenNodes.Count; i++)
                {
                    var whenNode = whenNodes[i];
                    when[i] = Analyze(whenNode, state);

                    if (!when[i].IsVoid() && when[i] is not ConstantExpression)
                    {
                        throw new CompilationException("CASE statement with a test variable requires WHEN clauses to be constant values", whenNode);
                    }

                    if (ExpressionTreeExtensions.TryAdjustReturnType(whenNode, when[i], switchVariable.Type, out var adjusted))
                    {
                        when[i] = adjusted;
                    }
                    else
                    {
                        throw new CompilationException(
                            string.Format(
                                "Could not adjust WHEN value type {0} to CASE argument type {1}",
                                when[i].Type.FullName, switchVariable.Type.FullName), whenNode);
                    }
                }

                var then = Analyze(thenNode, state);
                cases.Add(new Tuple<Expression[], Expression, ParseTreeNode>(when, then, thenNode));

                if (then.IsVoid())
                {
                    // if there is at least one "void" return value, resulting value must be nullable
                    mustReturnNullable = true;
                }
                else if (firstNonVoidThen == null)
                {
                    firstNonVoidThen = then;
                }
            }

            if (firstNonVoidThen == null && !caseDefault.IsVoid())
            {
                firstNonVoidThen = caseDefault;
            }

            var adjustedCaseDefault = caseDefault;

            // now try to adjust whatever remaining VOID "then-s" to the first-met non-void then
            // if all THENs are void, then just leave it as-is - type will be adjusted by caller
            if (firstNonVoidThen != null)
            {
                if (mustReturnNullable && firstNonVoidThen.Type.IsValueType && !firstNonVoidThen.IsNullableType())
                {
                    firstNonVoidThen = ExpressionTreeExtensions.MakeNewNullable(
                        typeof(UnboxableNullable<>).MakeGenericType(firstNonVoidThen.Type),
                        firstNonVoidThen);
                }

                for (var i = 0; i < cases.Count; i++)
                {
                    var thenNode = cases[i].Item3;
                    var then = cases[i].Item2;

                    if (!ReferenceEquals(then, firstNonVoidThen) && then.IsVoid())
                    {
                        if (ExpressionTreeExtensions.TryAdjustReturnType(thenNode, then, firstNonVoidThen.Type, out var adjusted))
                        {
                            cases[i] = new Tuple<Expression[], Expression, ParseTreeNode>(cases[i].Item1, adjusted, cases[i].Item3);
                        }
                        else
                        {
                            throw new CompilationException(
                                string.Format(
                                    "Could not adjust THEN value type {0} to first-met THEN value type {1}",
                                    then.Type.FullName, firstNonVoidThen.Type.FullName), thenNode);
                        }
                    }
                }

                if (caseDefault != null
                    && !ExpressionTreeExtensions.TryAdjustReturnType(caseVariableNode, caseDefault, firstNonVoidThen.Type, out adjustedCaseDefault))
                {
                    throw new CompilationException(
                        string.Format(
                            "Could not adjust CASE default value's type {0} to first-met THEN value type {1}",
                            caseDefault.Type.FullName, firstNonVoidThen.Type.FullName), caseVariableNode);
                }
            }

            if (adjustedCaseDefault == null)
            {
                adjustedCaseDefault = ExpressionTreeExtensions.GetDefaultExpression(
                    firstNonVoidThen == null
                        ? typeof(UnboxableNullable<ExpressionTreeExtensions.VoidTypeMarker>)
                        : firstNonVoidThen.Type);
            }

            return Expression.Switch(
                switchVariable, adjustedCaseDefault, null,
                cases.Select(x => Expression.SwitchCase(x.Item2, x.Item1)));
        }

        private Expression BuildCaseStatementExpression(CompilerState state, ParseTreeNode whenThenListNode, Expression caseDefault)
        {
            // now start building on top of the tail, right to left, 
            // also making sure that types are compatible
            var tail = caseDefault;

            for (var i = whenThenListNode.ChildNodes.Count - 1; i >= 0; i--)
            {
                var caseWhenThenNode = whenThenListNode.RequireChild("caseWhenThen", i);
                caseWhenThenNode.RequireChildren(4);
                var whenNode = caseWhenThenNode.RequireChild(null, 1);
                var thenNode = caseWhenThenNode.RequireChild(null, 3);

                if (whenNode.Term.Name == "tuple")
                {
                    throw new CompilationException("When variable for CASE is not specified, you can only have one expression in each WHEN clause", whenNode);
                }

                // in this flavor of CASE, we are building a sequence of IIFs
                // it requires that our "WHEN" clause is of non-nullable boolean type
                var when = Analyze(whenNode, state).RemoveNullability();
                when.RequireBoolean(whenNode);

                var then = Analyze(thenNode, state);

                // try to auto-adjust types of this "THEN" and current tail expression if needed
                if (tail != null)
                {
                    if (ExpressionTreeExtensions.TryAdjustReturnType(thenNode, then, tail.Type, out var adjusted))
                    {
                        then = adjusted;
                    }
                    else if (ExpressionTreeExtensions.TryAdjustReturnType(thenNode, tail, then.Type, out adjusted))
                    {
                        tail = adjusted;
                    }
                    else
                    {
                        throw new CompilationException(
                            string.Format(
                                "Incompatible types within CASE statement. Tail is of type {0}, and then is of type {1}",
                                tail.Type.FullName, then.Type.FullName), thenNode);
                    }
                }

                if (when is ConstantExpression)
                {
                    if ((bool)((ConstantExpression)when).Value)
                    {
                        tail = then;
                    }
                }
                else
                {
                    tail = Expression.Condition(when, then, tail ?? ExpressionTreeExtensions.GetDefaultExpression(then.Type));
                }
            }

            return tail;
        }

        private Expression BuildIsNullPredicate(ParseTreeNode root, CompilerState state, bool compareIsNull)
        {
            var value = state.ParentRuntime.Analyze(root, state);

            // IS NULL
            if (compareIsNull)
            {
                return value.Type.IsNullableType()
                           ? ConstantHelper.TryEvalConst(root, ConstantHelper.TryEvalConst(root, value.Type.GetField("HasValue"), value), ExpressionType.Not, typeof(bool))
                           : value.Type.IsValueType
                                 ? Expression.Constant(false)
                                 : ConstantHelper.TryEvalConst(root, value, Expression.Constant(null), ExpressionType.Equal);
            }

            // IS NOT NULL
            return value.Type.IsNullableType()
                       ? ConstantHelper.TryEvalConst(root, value.Type.GetField("HasValue"), value)
                       : value.Type.IsValueType
                             ? Expression.Constant(true)
                             : ConstantHelper.TryEvalConst(root, value, Expression.Constant(null), ExpressionType.NotEqual);
        }

        private Expression BuildUnaryExpression(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(2, 4);

            ParseTreeNode targetNode;
            string op;

            if (root.ChildNodes.Count == 2)
            {
                targetNode = root.ChildNodes[1];
                op = root.ChildNodes[0].Token.ValueString;
            }
            else
            {
                // we should have IS NULL or IS NOT NULL operator here
                targetNode = root.ChildNodes[0];
                var k1 = root.RequireChild("is", 1).Term.Name;
                var k2 = root.RequireChild(null, 2).Term.Name;
                var k3 = root.ChildNodes.Count == 4 ? root.RequireChild(null, 3).Term.Name : null;
                if (0 != StringComparer.OrdinalIgnoreCase.Compare(k1, "is")
                    || (k3 == null && 0 != StringComparer.OrdinalIgnoreCase.Compare(k2, "null"))
                    || (k3 != null && (0 != StringComparer.OrdinalIgnoreCase.Compare(k2, "not") || 0 != StringComparer.OrdinalIgnoreCase.Compare(k3, "null")))
                )
                {
                    throw new CompilationException("IS NULL or IS NOT NULL expected", root);
                }

                op = k3 == null ? "is null" : "is not null";
            }

            switch (op)
            {
                case "-":
                    {
                        var target = Analyze(targetNode, state).RemoveNullability();
                        target.RequireNumeric(targetNode);
                        return ConstantHelper.TryEvalConst(root, target, ExpressionType.Negate, target.Type);
                    }
                case "+":
                    {
                        var target = Analyze(targetNode, state).RemoveNullability();
                        target.RequireNumeric(targetNode);
                        return target;
                    }
                case "~":
                    {
                        var target = Analyze(targetNode, state).RemoveNullability();
                        target.RequireInteger(targetNode);
                        return ConstantHelper.TryEvalConst(root, target, ExpressionType.Not, target.Type);
                    }
                case "is null":
                    return BuildIsNullPredicate(targetNode, state, true);
                case "is not null":
                    return BuildIsNullPredicate(targetNode, state, false);
                default:
                    {
                        var target = Analyze(targetNode, state).RemoveNullability();
                        if (0 == StringComparer.OrdinalIgnoreCase.Compare("not", op))
                        {
                            target.RequireBoolean(targetNode);
                            return ConstantHelper.TryEvalConst(root, target, ExpressionType.Not, target.Type);
                        }
                        throw new CompilationException(
                            string.Format(
                                "Unary operator {0} not supported for type {1}", op, target.Type.FullName), root);
                    }
            }
        }

        private Expression BuildBetweenExpression(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(6);

            var variableNode = root.ChildNodes[0];
            var notOpt = root.RequireChild("notOpt", 1);
            root.RequireChild("between", 2);
            root.RequireChild("and", 4);

            var argument = Analyze(variableNode, state).RemoveNullability();

            var leftNode = root.ChildNodes[3];
            var leftExpr = Analyze(leftNode, state);
            leftExpr.RequireNonVoid(leftNode);
            leftExpr = ExpressionTreeExtensions.AdjustReturnType(leftNode, leftExpr, argument.Type);

            var rightNode = root.ChildNodes[5];
            var rightExpr = Analyze(rightNode, state);
            rightExpr.RequireNonVoid(rightNode);
            rightExpr = ExpressionTreeExtensions.AdjustReturnType(rightNode, rightExpr, argument.Type);

            Expression lower, upper;
            if (argument.IsString())
            {
                lower = ConstantHelper.TryEvalConst(leftNode, PrepareStringComparison(leftNode, leftExpr, argument), Expression.Constant(0), ExpressionType.LessThanOrEqual);
                upper = ConstantHelper.TryEvalConst(rightNode, PrepareStringComparison(rightNode, rightExpr, argument), Expression.Constant(0), ExpressionType.GreaterThanOrEqual);
            }
            else
            {
                lower = ConstantHelper.TryEvalConst(leftNode, argument, leftExpr, ExpressionType.GreaterThanOrEqual);
                upper = ConstantHelper.TryEvalConst(rightNode, argument, rightExpr, ExpressionType.LessThanOrEqual);
            }

            var result = ConstantHelper.TryEvalConst(root, lower, upper, ExpressionType.AndAlso);

            if (notOpt.ChildNodes.Count > 0)
            {
                result = ConstantHelper.TryEvalConst(root, result, ExpressionType.Not);
            }

            return result;
        }

        private Expression BuildBinaryExpression(ParseTreeNode root, CompilerState state)
        {
            root.RequireChildren(3);

            var leftNode = root.ChildNodes[0];
            var leftExpr = Analyze(leftNode, state);

            var op = GetBinaryOperator(root.ChildNodes[1]);
            if (op is "in" or "not in")
            {
                return BuildInclusionExpression(root, leftExpr, op, state);
            }

            var rightNode = root.ChildNodes[2];
            var rightExpr = Analyze(rightNode, state);

            if (!ExpressionTreeExtensions.TryAdjustVoid(ref leftExpr, ref rightExpr))
            {
                throw new CompilationException("This operation is not defined when both arguments are void", root);
            }

            Expression expr;

            leftExpr = leftExpr.RemoveNullability();
            rightExpr = rightExpr.RemoveNullability();

            if ((leftExpr.IsDateTime() && rightExpr.IsDateTime())
                || (leftExpr.IsTimeSpan() && rightExpr.IsTimeSpan()))
            {
                #region DateTime and DateTime, or TimeSpan and TimeSpan
                switch (op)
                {
                    case "+":
                        if (leftExpr.IsDateTime() && rightExpr.IsDateTime())
                        {
                            throw new CompilationException("Datetime values cannot be added to one another", root);
                        }
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Add);
                        break;
                    case "-":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Subtract);
                        break;
                    case "=":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Equal);
                        break;
                    case "!=":
                    case "<>":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.NotEqual);
                        break;
                    case ">":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.GreaterThan);
                        break;
                    case "<":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.LessThan);
                        break;
                    case "<=":
                    case "!>":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.LessThanOrEqual);
                        break;
                    case ">=":
                    case "!<":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.GreaterThanOrEqual);
                        break;
                    default:
                        throw new CompilationException("Binary operator not supported for datetime values: " + op, root.ChildNodes[1]);
                }
                #endregion
            }
            else if (leftExpr.IsDateTime() && rightExpr.IsTimeSpan())
            {
                #region DateTime and TimeSpan or TimeSpan and DateTime
                switch (op)
                {
                    case "+":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Add);
                        break;
                    case "-":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Subtract);
                        break;
                    default:
                        throw new CompilationException("Binary operator not supported for datetime and timespan: " + op, root.ChildNodes[1]);
                }
                #endregion
            }
            else if (leftExpr.IsTimeSpan() && rightExpr.IsDateTime())
            {
                #region TimeSpan and DateTime
                switch (op)
                {
                    case "+":
                        expr = ConstantHelper.TryEvalConst(root, rightExpr, leftExpr, ExpressionType.Add);
                        break;
                    default:
                        throw new CompilationException("Binary operator not supported for timespan and datetime: " + op, root.ChildNodes[1]);
                }
                #endregion
            }
            else if (leftExpr.IsString() && rightExpr.IsString())
            {
                #region String and String
                switch (op)
                {
                    case "+":
                        var concat = ReflectionHelper.StringConcat;
                        expr = ConstantHelper.TryEvalConst(root, concat, leftExpr, rightExpr);
                        break;

                    case "=":
                    case "!=":
                    case "<>":
                        expr = PrepareStringEquality(root, leftExpr, rightExpr);
                        if (op[0] != '=')
                        {
                            expr = ConstantHelper.TryEvalConst(root, expr, ExpressionType.Not, expr.Type);
                        }
                        break;

                    case ">":
                        expr = ConstantHelper.TryEvalConst(root, PrepareStringComparison(root, leftExpr, rightExpr), Expression.Constant(0), ExpressionType.GreaterThan);
                        break;

                    case "<":
                        expr = ConstantHelper.TryEvalConst(root, PrepareStringComparison(root, leftExpr, rightExpr), Expression.Constant(0), ExpressionType.LessThan);
                        break;

                    case "<=":
                    case "!>":
                        expr = ConstantHelper.TryEvalConst(root, PrepareStringComparison(root, leftExpr, rightExpr), Expression.Constant(0), ExpressionType.LessThanOrEqual);
                        break;

                    case ">=":
                    case "!<":
                        expr = ConstantHelper.TryEvalConst(root, PrepareStringComparison(root, leftExpr, rightExpr), Expression.Constant(0), ExpressionType.GreaterThanOrEqual);
                        break;

                    case "like":
                        throw new CompilationException("Instead of LIKE, use predefined functions StartsWith, EndsWith and Contains", root.ChildNodes[1]);
                    default:
                        throw new CompilationException("Binary operator not supported for strings: " + op, root.ChildNodes[1]);
                }
                #endregion
            }
            else if (leftExpr.IsNumeric() && rightExpr.IsNumeric())
            {
                #region Numeric and Numeric
                ExpressionTreeExtensions.AdjustArgumentsForBinaryOperation(ref leftExpr, ref rightExpr, root);

                switch (op)
                {
                    case "+":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Add);
                        break;

                    case "-":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Subtract);
                        break;

                    case "*":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Multiply);
                        break;

                    case "/":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Divide);
                        break;

                    case "%":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Modulo);
                        break;

                    case "&":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.And);
                        break;

                    case "|":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Or);
                        break;

                    case "^":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.ExclusiveOr);
                        break;

                    case "<":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.LessThan);
                        break;

                    case "<=":
                    case "!>":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.LessThanOrEqual);
                        break;

                    case ">":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.GreaterThan);
                        break;

                    case ">=":
                    case "!<":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.GreaterThanOrEqual);
                        break;

                    case "=":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Equal);
                        break;

                    case "!=":
                    case "<>":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.NotEqual);
                        break;

                    default:
                        throw new CompilationException("Binary operator not supported yet for numerics: " + op, root.ChildNodes[1]);
                }
                #endregion
            }
            else if (leftExpr.IsBoolean() && rightExpr.IsBoolean())
            {
                #region Boolean and Boolean
                switch (op)
                {
                    case "and":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.AndAlso);
                        break;
                    case "or":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.OrElse);
                        break;
                    case "xor":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.ExclusiveOr);
                        break;
                    case "=":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.Equal);
                        break;
                    case "!=":
                    case "<>":
                        expr = ConstantHelper.TryEvalConst(root, leftExpr, rightExpr, ExpressionType.NotEqual);
                        break;

                    default:
                        throw new CompilationException("Binary operator not supported yet for booleans: " + op, root.ChildNodes[1]);
                }
                #endregion
            }
            else
            {
                throw new CompilationException(string.Format("Binary operator {0} is not yet supported for types {1} and {2}",
                    op, leftExpr.Type.FullName, rightExpr.Type.FullName), root.ChildNodes[1]);
            }

            return expr;
        }

        private Expression BuildInclusionExpression(ParseTreeNode root, Expression leftExpr, string op, CompilerState state)
        {
            root.RequireChildren(3);

            var rightNodeList = ExpressionTreeExtensions.UnwindTupleExprList(root.ChildNodes[2]);
            if (rightNodeList.Term.Name == "Id")
            {
                throw new CompilationException("Parameterized IN statement is not yet supported, consider using function SetContains", rightNodeList);
            }

            if (rightNodeList.Term.Name != "exprList")
            {
                throw new CompilationException("Argument for IN operator must be a list of expressions", root);
            }

            // Expression text is not supposed to be used to pass tens and hundreds of thousands of IDs in plain text.
            // Use parameters for large argument sets.
            rightNodeList.RequireChildren(1, 1000);

            leftExpr = leftExpr.RemoveNullability();

            // compile a method to enumerate values in the argument set
            var valueEnumerator = ReflectionHelper.EnumerateValues.MakeGenericMethod(leftExpr.Type);
            // invoke enumerator, output is a hashset
            object matchingSet;
            try
            {
                matchingSet = valueEnumerator.Invoke(null, new object[] { this, rightNodeList, state });
            }
            catch (TargetInvocationException e)
            {
                if (e.InnerException == null) throw;
                throw e.InnerException;
            }

            // how many items do we have there?
            var countProperty = matchingSet.GetType().GetProperty("Count", BindingFlags.Instance | BindingFlags.Public);
            var count = (int)(countProperty.GetValue(matchingSet));

            Expression contains;
            if (leftExpr is ConstantExpression leftArgConst)
            {
                // since list is constant and argument is constant, let's just evaluate it
                var setContainsMethod = ReflectionHelper.GetOrAddMethod1(matchingSet.GetType(), "Contains", leftExpr.Type);

                try
                {
                    contains = Expression.Constant(setContainsMethod.Invoke(matchingSet, new[] { leftArgConst.Value }), typeof(bool));
                }
                catch (TargetInvocationException e)
                {
                    if (e.InnerException == null)
                        throw;
                    throw e.InnerException;
                }
            }
            else
            {
                var threshold = leftExpr.IsInteger() ? 15 : 5;

                // for small sets of values, just create a chain of IF/THEN/ELSE statements
                if (count <= threshold)
                {
                    var isString = leftExpr.IsString();
                    var enumeratorMethod = ReflectionHelper.GetOrAddMethod0(matchingSet.GetType(), "GetEnumerator");
                    IEnumerator enumerator;
                    try
                    {
                        enumerator = (IEnumerator)enumeratorMethod.Invoke(matchingSet, null);
                    }
                    catch (TargetInvocationException e)
                    {
                        if (e.InnerException == null)
                            throw;
                        throw e.InnerException;
                    }

                    contains = null;
                    while (enumerator.MoveNext())
                    {
                        var next = isString
                            ? PrepareStringEquality(rightNodeList, leftExpr, Expression.Constant(enumerator.Current, leftExpr.Type))
                            : Expression.Equal(leftExpr, Expression.Constant(enumerator.Current, leftExpr.Type));
                        contains = contains == null ? next : Expression.OrElse(contains, next);
                    }
                }
                else
                {
                    // for larger sets, wire our matchingSet into this expression as constant reference
                    // it will be kept alive by garbage collector, and will be collected when expression is collected
                    var setContainsMethod = ReflectionHelper.GetOrAddMethod1(matchingSet.GetType(), "Contains", leftExpr.Type);
                    contains = Expression.Call(Expression.Constant(matchingSet), setContainsMethod, leftExpr);
                }
            }

            if (op.StartsWith("not "))
            {
                contains = ConstantHelper.TryEvalConst(root, contains, ExpressionType.Not, typeof(bool));
            }

            return contains;
        }

        private static Expression PrepareStringComparison(ParseTreeNode root, Expression leftExpr, Expression rightExpr)
        {
            var compare = ReflectionHelper.StringComparerCompare;
            return ConstantHelper.TryEvalConst(root, compare, Expression.Constant(StringComparer.OrdinalIgnoreCase), leftExpr, rightExpr);
        }

        private static Expression PrepareStringEquality(ParseTreeNode root, Expression leftExpr, Expression rightExpr)
        {
            var equals = ReflectionHelper.StringComparerEquals;
            return ConstantHelper.TryEvalConst(root, equals, Expression.Constant(StringComparer.OrdinalIgnoreCase), leftExpr, rightExpr);
        }

        private string GetBinaryOperator(ParseTreeNode opNode)
        {
            string text;

            opNode.RequireChildren(1, 2);
            if (opNode.ChildNodes.Count == 1)
            {
                text = opNode.ChildNodes[0].Token.ValueString;
                return text.IsKeywordAffectedByCase() ? text.ToLower() : text;
            }

            // handle cases "NOT LIKE", "NOT IN"
            text = opNode.ChildNodes[0].Token.ValueString + ' ' + opNode.ChildNodes[1].Token.ValueString;
            return text.IsKeywordAffectedByCase() ? text.ToLower() : text;
        }

        private object CompileImpl(Expression expression, CompilerState state)
        {
            if (state == null)
            {
                throw new ArgumentNullException(nameof(state));
            }

            state.RawReturnType = expression.Type;

            while (expression.CanReduce)
            {
                expression = expression.Reduce();
            }

            LambdaExpression lambda;
            var paramTypes = state.CompileToAction ? new Type[state.Arguments.Count] : new Type[state.Arguments.Count + 1];
            for (var i = 0; i < state.Arguments.Count; i++)
            {
                paramTypes[i] = state.Arguments[i].Type;
            }

            if (state.CompileToAction)
            {
                lambda = Expression.Lambda(Expression.GetActionType(paramTypes), expression, state.Arguments);
            }
            else
            {
                var returnType = state.ReturnType ?? state.RawReturnType;
                paramTypes[paramTypes.Length - 1] = returnType;

                expression = ExpressionTreeExtensions.AdjustReturnType(null, expression, returnType);
                lambda = Expression.Lambda(expression, state.Arguments);

                if (!ReferenceEquals(returnType, lambda.ReturnType))
                {
                    throw new CompilationException(string.Format("Expected return type: {0}. Real return type: {1}", returnType.FullName, lambda.ReturnType.FullName));
                }
            }

            if (lambda.Parameters.Count != state.Arguments.Count)
            {
                throw new CompilationException(string.Format("Expected number of arguments: {0}. Real number: {1}", state.Arguments.Count, lambda.Parameters.Count));
            }

            for (var i = 0; i < lambda.Parameters.Count; i++)
            {
                if (!ReferenceEquals(lambda.Parameters[i].Type, state.Arguments[i].Type))
                {
                    throw new CompilationException(
                        string.Format("Expected type of argument {0}: {1}. Real type: {2}", i, state.Arguments[i].Type.FullName, lambda.Parameters[i].Type.FullName));
                }
            }

            return lambda.Compile();
        }
    }
}
