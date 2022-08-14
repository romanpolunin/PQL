using Irony.Parsing;

namespace Pql.ExpressionEngine.Grammar
{
    [Language("Expression", "1.0", "Expression grammar")]
    internal class ExpressionGrammar : Irony.Parsing.Grammar
    {
        public ExpressionGrammar()
            : base(false)
        {
            //SQL is case insensitive
            //Terminals
            var comment = new CommentTerminal("comment", "/*", "*/");
            var lineComment = new CommentTerminal("line_comment", "--", "\n", "\r\n");
            NonGrammarTerminals.Add(comment);
            NonGrammarTerminals.Add(lineComment);
            var numberLiteral = new NumberLiteral("number", NumberOptions.AllowSign | NumberOptions.AllowStartEndDot) { DefaultIntTypes = new[] { TypeCode.Int32, TypeCode.Int64 } };
            var stringLiteral = new StringLiteral("string", "'", StringOptions.AllowsDoubledQuote | StringOptions.AllowsLineBreak | StringOptions.AllowsAllEscapes);
            var idSimple = CreateSqlExtIdentifier("id_simple"); //covers normal identifiers (abc) and quoted id's ([abc d], "abc d")
            var commaTerm = ToTerm(",");
            var dotTerm = ToTerm(".");
            var notTerm = ToTerm("NOT");

            //Non-terminals
            var id = new NonTerminal("Id");
            var idlist = new NonTerminal("idlist");
            var idlistPar = new NonTerminal("idlistPar");
            var tuple = new NonTerminal("tuple");
            var expression = new NonTerminal("expression");
            var exprList = new NonTerminal("exprList");
            var caseStmt = new NonTerminal("case");
            var caseVariable = new NonTerminal("caseVariable");
            var caseWhenList = new NonTerminal("caseWhenList");
            var caseWhenThen = new NonTerminal("caseWhenThen");
            var caseDefaultOpt = new NonTerminal("caseDefault");
            var term = new NonTerminal("term");
            var unExpr = new NonTerminal("unExpr");
            var unOp = new NonTerminal("unOp");
            var binExpr = new NonTerminal("binExpr");
            var binOp = new NonTerminal("binOp");
            var betweenExpr = new NonTerminal("betweenExpr");
            //var inExpr = new NonTerminal("inExpr");
            var notOpt = new NonTerminal("notOpt");
            var funCall = new NonTerminal("funCall");
            var funArgs = new NonTerminal("funArgs");
            //var inStmt = new NonTerminal("inStmt");

            //BNF Rules
            Root = expression;

            //ID
            id.Rule = MakePlusRule(id, dotTerm, idSimple);

            idlistPar.Rule = "(" + idlist + ")";
            idlist.Rule = MakePlusRule(idlist, commaTerm, id);

            caseWhenThen.Rule = "WHEN" + exprList + "THEN" + expression;
            caseWhenList.Rule = MakePlusRule(caseWhenList, caseWhenThen);
            caseDefaultOpt.Rule = Empty | ("ELSE" + expression);
            caseVariable.Rule = Empty | expression;
            caseStmt.Rule = "CASE" + caseVariable + caseWhenList + caseDefaultOpt + "END";

            //Expression
            exprList.Rule = MakePlusRule(exprList, commaTerm, expression);
            expression.Rule = term | unExpr | binExpr | caseStmt | betweenExpr; //-- BETWEEN brings a few parsing conflicts, use parentheses
            tuple.Rule = "(" + exprList + ")";
            term.Rule = id | stringLiteral | numberLiteral | funCall | tuple;// | inStmt;
            unExpr.Rule = (unOp + term) | (term + "IS" + "NULL") | (term + "IS" + "NOT" + "NULL");
            unOp.Rule = notTerm | "+" | "-" | "~";
            binExpr.Rule = expression + binOp + expression;
            binOp.Rule = ToTerm("+") | "-" | "*" | "/" | "%" //arithmetic
                        | "&" | "|" | "^"                     //bit
                        | "=" | ">" | "<" | ">=" | "<=" | "<>" | "!=" | "!<" | "!>"
                        | "AND" | "OR" | "XOR" | "LIKE" | (notTerm + "LIKE") | "IN" | (notTerm + "IN");
            betweenExpr.Rule = expression + notOpt + "BETWEEN" + expression + "AND" + expression;
            notOpt.Rule = Empty | notTerm;
            //funCall covers some psedo-operators and special forms like ANY(...), SOME(...), ALL(...), EXISTS(...), IN(...)
            funCall.Rule = id + "(" + funArgs + ")";
            funArgs.Rule = Empty | exprList;
            //inStmt.Rule = expression + "IN" + "(" + exprList + ")";

            //Operators
            RegisterOperators(10, "*", "/", "%");
            RegisterOperators(9, "+", "-");
            RegisterOperators(8, "=", ">", "<", ">=", "<=", "<>", "!=", "!<", "!>", "LIKE", "IN", "BETWEEN");
            RegisterOperators(7, "^", "&", "|");
            RegisterOperators(6, notTerm);
            RegisterOperators(5, "AND");
            RegisterOperators(4, "OR", "XOR");

            MarkPunctuation(",", "(", ")");
            //Note: we cannot declare binOp as transient because it includes operators "NOT LIKE", "NOT IN" consisting of two tokens. 
            // Transient non-terminals cannot have more than one non-punctuation child nodes.
            // Instead, we set flag InheritPrecedence on binOp , so that it inherits precedence value from it's children, and this precedence is used
            // in conflict resolution when binOp node is sitting on the stack
            MarkTransient(term, expression, unOp);
            binOp.SetFlag(TermFlags.InheritPrecedence);

            SnippetRoots.Add(expression);
        }

        private BnfTerm CreateSqlExtIdentifier(string name)
        {
            var identifierTerminal = new IdentifierTerminal(name, "_", "@_");
            var stringLiteral = new StringLiteral(name + "_quoted");
            stringLiteral.AddStartEnd("[", "]", StringOptions.NoEscapes);
            stringLiteral.AddStartEnd("\"", StringOptions.NoEscapes);
            stringLiteral.SetOutputTerminal(this, identifierTerminal);
            return identifierTerminal;
        }
    }
}
