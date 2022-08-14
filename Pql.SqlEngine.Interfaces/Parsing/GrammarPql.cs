using Irony.Parsing;

namespace Pql.SqlEngine.Interfaces.Parsing
{
    // Loosely based on SQL89 grammar from Gold parser. Supports some extra TSQL constructs.

    [Language("PQL", "1.0", "PQL 1.0 grammar")]
    public class GrammarPql : Irony.Parsing.Grammar
    {
        public GrammarPql()
            : base(false)
        { //SQL is case insensitive
            //Terminals
            var comment = new CommentTerminal("comment", "/*", "*/");
            var lineComment = new CommentTerminal("line_comment", "--", "\n", "\r\n");
            NonGrammarTerminals.Add(comment);
            NonGrammarTerminals.Add(lineComment);
            var number = new NumberLiteral("number", NumberOptions.AllowSign | NumberOptions.AllowStartEndDot) { DefaultIntTypes = new[] { TypeCode.Int32, TypeCode.Int64 } };
            var string_literal = new StringLiteral("string", "'", StringOptions.AllowsDoubledQuote | StringOptions.AllowsLineBreak | StringOptions.AllowsAllEscapes);
            var id_simple = CreateSqlExtIdentifier("id_simple"); //covers normal identifiers (abc) and quoted id's ([abc d], "abc d")
            var comma = ToTerm(",");
            var dot = ToTerm(".");
            //var CREATE = ToTerm("CREATE");
            var termNULL = ToTerm("NULL");
            var termNOT = ToTerm("NOT");
            //var UNIQUE = ToTerm("UNIQUE");
            //var WITH = ToTerm("WITH");
            //var TABLE = ToTerm("TABLE");
            //var ALTER = ToTerm("ALTER");
            //var ADD = ToTerm("ADD");
            //var COLUMN = ToTerm("COLUMN");
            //var DROP = ToTerm("DROP");
            //var CONSTRAINT = ToTerm("CONSTRAINT");
            //var INDEX = ToTerm("INDEX");
            var termON = ToTerm("ON");
            //var KEY = ToTerm("KEY");
            //var PRIMARY = ToTerm("PRIMARY");
            var termINSERT = ToTerm("INSERT");
            var termINTO = ToTerm("INTO");
            var termUPDATE = ToTerm("UPDATE");
            var termSET = ToTerm("SET");
            var termVALUES = ToTerm("VALUES");
            var termDELETE = ToTerm("DELETE");
            var termSELECT = ToTerm("SELECT");
            var termFROM = ToTerm("FROM");
            var termAS = ToTerm("AS");
            var termCOUNT = ToTerm("COUNT");
            var termJOIN = ToTerm("JOIN");
            var termBY = ToTerm("BY");

            //Non-terminals
            var id = new NonTerminal("Id");
            var stmt = new NonTerminal("stmt");
            //var createTableStmt = new NonTerminal("createTableStmt");
            //var createIndexStmt = new NonTerminal("createIndexStmt");
            //var alterStmt = new NonTerminal("alterStmt");
            //var dropTableStmt = new NonTerminal("dropTableStmt");
            //var dropIndexStmt = new NonTerminal("dropIndexStmt");
            var selectStmt = new NonTerminal("selectStmt");
            var insertStmt = new NonTerminal("insertStmt");
            var updateStmt = new NonTerminal("updateStmt");
            var deleteStmt = new NonTerminal("deleteStmt");
            //var fieldDef = new NonTerminal("fieldDef");
            //var fieldDefList = new NonTerminal("fieldDefList");
            var nullSpecOpt = new NonTerminal("nullSpecOpt");
            //var typeName = new NonTerminal("typeName");
            //var typeSpec = new NonTerminal("typeSpec");
            //var typeParamsOpt = new NonTerminal("typeParams");
            //var constraintDef = new NonTerminal("constraintDef");
            //var constraintListOpt = new NonTerminal("constraintListOpt");
            //var constraintTypeOpt = new NonTerminal("constraintTypeOpt");
            var idlist = new NonTerminal("idlist");
            var idlistPar = new NonTerminal("idlistPar");
            //var uniqueOpt = new NonTerminal("uniqueOpt");
            var orderList = new NonTerminal("orderList");
            var orderMember = new NonTerminal("orderMember");
            var orderDirOpt = new NonTerminal("orderDirOpt");
            //var withClauseOpt = new NonTerminal("withClauseOpt");
            //var alterCmd = new NonTerminal("alterCmd");
            var insertData = new NonTerminal("insertData");
            var insertDataTuplesList = new NonTerminal("insertTuplesList");
            var intoOpt = new NonTerminal("intoOpt");
            var assignList = new NonTerminal("assignList");
            var whereClauseOpt = new NonTerminal("whereClauseOpt");
            var assignment = new NonTerminal("assignment");
            var expression = new NonTerminal("expression");
            var exprList = new NonTerminal("exprList");
            var selRestrOpt = new NonTerminal("selRestrOpt");
            var selList = new NonTerminal("selList");
            var intoClauseOpt = new NonTerminal("intoClauseOpt");
            var fromClauseOpt = new NonTerminal("fromClauseOpt");
            var groupClauseOpt = new NonTerminal("groupClauseOpt");
            var havingClauseOpt = new NonTerminal("havingClauseOpt");
            var orderClauseOpt = new NonTerminal("orderClauseOpt");
            var columnItemList = new NonTerminal("columnItemList");
            var columnItem = new NonTerminal("columnItem");
            var columnSource = new NonTerminal("columnSource");
            var asOpt = new NonTerminal("asOpt");
            var aliasOpt = new NonTerminal("aliasOpt");
            var caseStmt = new NonTerminal("case");
            var caseVariable = new NonTerminal("caseVariable");
            var caseWhenList = new NonTerminal("caseWhenList");
            var caseWhenThen = new NonTerminal("caseWhenThen");
            var caseDefaultOpt = new NonTerminal("caseDefault");
            var aggregate = new NonTerminal("aggregate");
            var aggregateArg = new NonTerminal("aggregateArg");
            var aggregateName = new NonTerminal("aggregateName");
            var tuple = new NonTerminal("tuple");
            var joinChainOpt = new NonTerminal("joinChainOpt");
            var joinKindOpt = new NonTerminal("joinKindOpt");
            var term = new NonTerminal("term");
            var unExpr = new NonTerminal("unExpr");
            var unOp = new NonTerminal("unOp");
            var binExpr = new NonTerminal("binExpr");
            var binOp = new NonTerminal("binOp");
            var betweenExpr = new NonTerminal("betweenExpr");
            //var inExpr = new NonTerminal("inExpr");
            var parSelectStmt = new NonTerminal("parSelectStmt");
            var notOpt = new NonTerminal("notOpt");
            var funCall = new NonTerminal("funCall");
            var stmtLine = new NonTerminal("stmtLine");
            var semiOpt = new NonTerminal("semiOpt");
            var stmtList = new NonTerminal("stmtList");
            var funArgs = new NonTerminal("funArgs");
            var pageOffsetOpt = new NonTerminal("pageOffset");
            var pageSizeOpt = new NonTerminal("pageSize");

            //BNF Rules
            Root = stmtList;
            stmtLine.Rule = stmt + semiOpt;
            semiOpt.Rule = Empty | ";";
            stmtList.Rule = MakePlusRule(stmtList, stmtLine);

            //ID
            id.Rule = MakePlusRule(id, dot, id_simple);

            stmt.Rule = //createTableStmt | createIndexStmt | alterStmt
                        //| dropTableStmt | dropIndexStmt
                        selectStmt | insertStmt | updateStmt | deleteStmt
                        | "GO";
            //Create table
            //createTableStmt.Rule = CREATE + TABLE + Id + "(" + fieldDefList + ")" + constraintListOpt;
            //fieldDefList.Rule = MakePlusRule(fieldDefList, comma, fieldDef);
            //fieldDef.Rule = Id + typeName + typeParamsOpt + nullSpecOpt;
            nullSpecOpt.Rule = termNULL | (termNOT + termNULL) | Empty;
            //typeName.Rule = ToTerm("BOOL") | "DATETIME" | "DECIMAL" | "DOUBLE" | "INT16" | "INT64" | "NTEXT" | "VARBINARY";
            //typeName.Rule = ToTerm("BIT") | "DATE" | "TIME" | "TIMESTAMP" | "DECIMAL" | "REAL" | "FLOAT" | "SMALLINT" | "INTEGER"
            //                                | "INTERVAL" | "CHARACTER"
            //    // MS SQL types:  
            //                                | "DATETIME" | "INT" | "DOUBLE" | "CHAR" | "NCHAR" | "VARCHAR" | "NVARCHAR"
            //                                | "IMAGE" | "TEXT" | "NTEXT";
            //typeParamsOpt.Rule = "(" + number + ")" | "(" + number + comma + number + ")" | Empty;
            //constraintDef.Rule = CONSTRAINT + Id + constraintTypeOpt;
            //constraintListOpt.Rule = MakeStarRule(constraintListOpt, constraintDef);
            //constraintTypeOpt.Rule = PRIMARY + KEY + idlistPar | UNIQUE + idlistPar | NOT + NULL + idlistPar
            //                        | "Foreign" + KEY + idlistPar + "References" + Id + idlistPar;
            idlistPar.Rule = "(" + idlist + ")";
            idlist.Rule = MakePlusRule(idlist, comma, id);

            //Create Index
            //createIndexStmt.Rule = CREATE + uniqueOpt + INDEX + Id + ON + Id + orderList + withClauseOpt;
            //uniqueOpt.Rule = Empty | UNIQUE;
            orderList.Rule = MakePlusRule(orderList, comma, orderMember);
            orderMember.Rule = id + orderDirOpt;
            orderDirOpt.Rule = Empty | "ASC" | "DESC";
            //withClauseOpt.Rule = Empty | WITH + PRIMARY | WITH + "Disallow" + NULL | WITH + "Ignore" + NULL;

            //Alter 
            //alterStmt.Rule = ALTER + TABLE + Id + alterCmd;
            //alterCmd.Rule = ADD + COLUMN + fieldDefList + constraintListOpt
            //                | ADD + constraintDef
            //                | DROP + COLUMN + Id
            //                | DROP + CONSTRAINT + Id;

            ////Drop stmts
            //dropTableStmt.Rule = DROP + TABLE + Id;
            //dropIndexStmt.Rule = DROP + INDEX + Id + ON + Id;

            //Insert stmt
            insertStmt.Rule = termINSERT + intoOpt + id + idlistPar + insertData;
            insertData.Rule = selectStmt | (termVALUES + insertDataTuplesList);
            insertDataTuplesList.Rule = MakePlusRule(insertDataTuplesList, comma, tuple);
            intoOpt.Rule = Empty | termINTO; //Into is optional in MSSQL

            //Update stmt
            updateStmt.Rule = termUPDATE + id + termSET + assignList + whereClauseOpt + orderClauseOpt;
            assignList.Rule = MakePlusRule(assignList, comma, assignment);
            assignment.Rule = id + "=" + expression;

            //Delete stmt
            deleteStmt.Rule = termDELETE + termFROM + id + whereClauseOpt + orderClauseOpt;

            //Select stmt
            selectStmt.Rule = termSELECT + selRestrOpt + selList + intoClauseOpt + fromClauseOpt + whereClauseOpt +
                                groupClauseOpt + havingClauseOpt + orderClauseOpt + pageSizeOpt + pageOffsetOpt;
            selRestrOpt.Rule = Empty | "ALL" | "DISTINCT";
            selList.Rule = columnItemList | "*";
            columnItemList.Rule = MakePlusRule(columnItemList, comma, columnItem);
            columnItem.Rule = columnSource + aliasOpt;
            aliasOpt.Rule = Empty | (asOpt + id);
            asOpt.Rule = Empty | termAS;
            columnSource.Rule = aggregate | id | expression;
            caseWhenThen.Rule = "WHEN" + exprList + "THEN" + expression;
            caseWhenList.Rule = MakePlusRule(caseWhenList, caseWhenThen);
            caseDefaultOpt.Rule = Empty | ("ELSE" + expression);
            caseVariable.Rule = Empty | expression;
            caseStmt.Rule = "CASE" + caseVariable + caseWhenList + caseDefaultOpt + "END";
            aggregate.Rule = aggregateName + "(" + aggregateArg + ")";
            aggregateArg.Rule = expression | "*";
            aggregateName.Rule = termCOUNT | "Avg" | "Min" | "Max" | "StDev" | "StDevP" | "Sum" | "Var" | "VarP";
            intoClauseOpt.Rule = Empty | (termINTO + id);
            fromClauseOpt.Rule = Empty | (termFROM + idlist + joinChainOpt);
            joinChainOpt.Rule = Empty | (joinKindOpt + termJOIN + idlist + termON + id + "=" + id);
            joinKindOpt.Rule = Empty | "INNER" | "LEFT" | "RIGHT";
            whereClauseOpt.Rule = Empty | ("WHERE" + expression);
            groupClauseOpt.Rule = Empty | ("GROUP" + termBY + idlist);
            havingClauseOpt.Rule = Empty | ("HAVING" + expression);
            orderClauseOpt.Rule = Empty | ("ORDER" + termBY + orderList);
            pageOffsetOpt.Rule = Empty | ("OFFSET" + number) | ("OFFSET" + id);
            pageSizeOpt.Rule = Empty | ("LIMIT" + number) | ("LIMIT" + id);

            //Expression
            exprList.Rule = MakePlusRule(exprList, comma, expression);
            expression.Rule = term | unExpr | binExpr | caseStmt | betweenExpr; 
            term.Rule = id | string_literal | number | funCall | tuple | parSelectStmt;
            tuple.Rule = "(" + exprList + ")";
            parSelectStmt.Rule = "(" + selectStmt + ")";
            unExpr.Rule = (unOp + term) | (term + "IS" + "NULL") | (term + "IS" + "NOT" + "NULL");
            unOp.Rule = termNOT | "+" | "-" | "~";
            binExpr.Rule = expression + binOp + expression;
            binOp.Rule = ToTerm("+") | "-" | "*" | "/" | "%" //arithmetic
                        | "&" | "|" | "^"                     //bit
                        | "=" | ">" | "<" | ">=" | "<=" | "<>" | "!=" | "!<" | "!>"
                        | "AND" | "OR" | "XOR" | "LIKE" | (termNOT + "LIKE") | "IN" | (termNOT + "IN");
            betweenExpr.Rule = expression + notOpt + "BETWEEN" + expression + "AND" + expression;
            notOpt.Rule = Empty | termNOT;
            //funCall covers some psedo-operators and special forms like ANY(...), SOME(...), ALL(...), EXISTS(...), IN(...)
            funCall.Rule = id + "(" + funArgs + ")";
            funArgs.Rule = Empty | selectStmt | exprList;

            //Operators
            RegisterOperators(10, "*", "/", "%");
            RegisterOperators(9, "+", "-");
            RegisterOperators(8, "=", ">", "<", ">=", "<=", "<>", "!=", "!<", "!>", "LIKE", "IN", "BETWEEN");
            RegisterOperators(7, "^", "&", "|");
            RegisterOperators(6, termNOT);
            RegisterOperators(5, "AND");
            RegisterOperators(4, "OR", "XOR");

            MarkPunctuation(",", "(", ")");
            MarkPunctuation(asOpt, semiOpt);
            //Note: we cannot declare binOp as transient because it includes operators "NOT LIKE", "NOT IN" consisting of two tokens. 
            // Transient non-terminals cannot have more than one non-punctuation child nodes.
            // Instead, we set flag InheritPrecedence on binOp , so that it inherits precedence value from it's children, and this precedence is used
            // in conflict resolution when binOp node is sitting on the stack
            MarkTransient(stmt, term, asOpt, aliasOpt, stmtLine, expression, unOp, tuple);
            binOp.SetFlag(TermFlags.InheritPrecedence);

            SnippetRoots.Add(expression);

        }

        private BnfTerm CreateSqlExtIdentifier(string name)
        {
            var identifierTerminal = new IdentifierTerminal(name, null, "@");
            var stringLiteral = new StringLiteral(name + "_quoted");
            stringLiteral.AddStartEnd("[", "]", StringOptions.NoEscapes);
            stringLiteral.AddStartEnd("\"", StringOptions.NoEscapes);
            stringLiteral.SetOutputTerminal(this, identifierTerminal);
            return identifierTerminal;
        }
    }
}
