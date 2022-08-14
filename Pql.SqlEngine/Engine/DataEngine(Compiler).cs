using System.Data;
using System.Linq.Expressions;

using Pql.SqlEngine.DataContainer.Parser;
using Pql.ExpressionEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;
using Pql.SqlEngine.Interfaces.Services;

namespace Pql.SqlEngine.DataContainer.Engine
{
    public sealed partial class DataEngine
    {
        private static void CompileInsertUpdateClauses(
            IStorageDriver storageDriver, DataContainerDescriptor containerDescriptor, RequestExecutionContextCacheInfo cacheInfo, DriverChangeType changeType)
        {
            var updates = cacheInfo.ParsedRequest.Modify.UpdateAssignments;
            var clauses = cacheInfo.ParsedRequest.Modify.InsertUpdateSetClauses;
            var fields = cacheInfo.ParsedRequest.Modify.ModifiedFields;

            if (clauses.Count != fields.Count)
            {
                throw new Exception(string.Format("Internal error: insert/update clauses count ({0}) does not match count of modified fields ({1})",
                    clauses.Count, fields.Count));
            }

            // compile field assignment clauses (SET clauses or value inserts)
            for (var ordinal = 0; ordinal < clauses.Count; ordinal++)
            {
                var clause = clauses[ordinal];
                var field = fields[ordinal];

                // for bulk requests, primary key is there but it is only used to lookup the record
                // for non-bulk requests, primary key should not be in the list of UPDATE clauses
                if (changeType == DriverChangeType.Update && !cacheInfo.ParsedRequest.IsBulk)
                {
                    if (!storageDriver.CanUpdateField(field.FieldId))
                    {
                        throw new Exception(string.Format("Cannot update field {0}/{1} on entity {2}", field.FieldId, field.Name, cacheInfo.ParsedRequest.TargetEntity.Name));
                    }
                }

                // prepare Action compilation context
                var compilerState = QueryParser.PrepareCompilerState(containerDescriptor, cacheInfo, null);
                compilerState.CompileToAction = true;

                // extractor has signature like Func<ClauseEvaluationContext, T>
                var extractor = clause == null
                                    ? QueryParser.CompileFieldValueExtractorClause(compilerState, field, containerDescriptor, cacheInfo, MakeNullableType(field.DbType))
                                    : QueryParser.CompileClause(compilerState, clause, containerDescriptor, cacheInfo, MakeNullableType(field.DbType));
                // get the value into local variable, to prevent multiple invokations when row writer checks for null
                var extractedValue = Expression.Variable(extractor.Type);

                // now take the extractor and create another method, that will take the value and then put it into the changebuffer's data
                var changeBufferData = Expression.Field(Expression.Field(compilerState.Context, "ChangeBuffer"), "Data");
                var blockBody = Expression.Block(
                    new[] {extractedValue},
                    Expression.Assign(extractedValue, extractor),
                    DriverRowData.CreateWriteAccessor(extractedValue, changeBufferData, field.DbType, ordinal));

                updates.Add(
                    new ParsedRequest.FieldAssignment
                    {
                        Field = field,
                        CompiledExpression = (Action<ClauseEvaluationContext>)QueryParser.CompileClause(blockBody, compilerState)
                    });
            }
        }

        private static void CompileSelectClauses(DataContainerDescriptor containerDescriptor, RequestExecutionContextCacheInfo cacheInfo)
        {
            var parsedRequest = cacheInfo.ParsedRequest;

            if (parsedRequest.IsBulk)
            {
                foreach (var field in parsedRequest.Select.SelectFields) 
                {
                    var exprType = MakeNullableType(field.DbType);

                    parsedRequest.Select.OutputColumns.Add(
                        new ParsedRequest.SelectOutputColumn
                            {
                                Label = field.Name,
                                CompiledExpression = QueryParser.CompileFieldValueExtractorClause(field, containerDescriptor, cacheInfo, exprType),
                                DbType = field.DbType,
                                IsNullable = exprType.IsNullableType()
                            });
                }
            }
            else
            {
                foreach (var clause in parsedRequest.Select.SelectClauses) 
                {
                    // under column item, there is a "columnSource" element (with a single child), and optional "Id" element for alias
                    var columnExpressionNode = clause.RequireChild("columnSource", 0).RequireChild(null, 0);
                    var compiled = QueryParser.CompileClause(columnExpressionNode, containerDescriptor, cacheInfo, null);
                    var returnType = compiled.GetType().GetGenericArguments()[1];
                    var dbType = DriverRowData.DeriveDataType(returnType.GetUnderlyingType());

                    // get alias
                    var aliasNode = clause.TryGetChild("Id", 1) ?? columnExpressionNode;
                    var span = aliasNode.Span;
                    var label = span.Length > 0
                                    ? cacheInfo.CommandText.Substring(span.Location.Position, span.Length)
                                    : aliasNode.FindTokenAndGetText();

                    parsedRequest.Select.OutputColumns.Add(
                        new ParsedRequest.SelectOutputColumn
                            {
                                Label = label,
                                CompiledExpression = compiled,
                                DbType = dbType,
                                IsNullable = returnType.IsNullableType()
                            });
                }
            }
        }

        private void CompileClauses(DataContainerDescriptor containerDescriptor, RequestExecutionContextCacheInfo cacheInfo)
        {
            if (cacheInfo.ParsedRequest.SpecialCommand.IsSpecialCommand)
            {
                return;
            }

            if (cacheInfo.ParsedRequest.BaseDataset.WhereClauseRoot != null)
            {
                cacheInfo.ParsedRequest.BaseDataset.WhereClauseProcessor =
                    (Func<ClauseEvaluationContext, bool>) QueryParser.CompileWhereClause(
                        containerDescriptor,
                        cacheInfo.ParsedRequest.BaseDataset.WhereClauseRoot,
                        cacheInfo);
            }

            switch (cacheInfo.ParsedRequest.StatementType)
            {
                case StatementType.Select:
                    CompileSelectClauses(containerDescriptor, cacheInfo);
                    break;
                case StatementType.Update:
                    CompileInsertUpdateClauses(_storageDriver, containerDescriptor, cacheInfo, DriverChangeType.Update);
                    break;
                case StatementType.Insert:
                    CompileInsertUpdateClauses(_storageDriver, containerDescriptor, cacheInfo, DriverChangeType.Insert);
                    break;
                case StatementType.Delete:
                    break;
                default:
                    throw new Exception("Invalid statement type: " + cacheInfo.ParsedRequest.StatementType);
            }
        }

        private static Type MakeNullableType(DbType dbType)
        {
            var underlyingType = DriverRowData.DeriveSystemType(dbType);
            return underlyingType.IsValueType ? typeof(UnboxableNullable<>).MakeGenericType(underlyingType) : underlyingType;
        }
    }
}