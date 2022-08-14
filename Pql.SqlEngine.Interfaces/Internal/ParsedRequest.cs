using System.Data;
using System.Linq.Expressions;

using Irony.Parsing;

using Pql.SqlEngine.Interfaces.Services;

namespace Pql.SqlEngine.Interfaces.Internal
{
    public class ClauseEvaluationContext
    {
        /// <summary>
        /// Zero-based number of current row, counts all records that come from storage driver, AFTER sorting.
        /// Is not affected by filtering and paging. Exposed in PQL via "rownum()" function.
        /// </summary>
        public long RowNumber;
        /// <summary>
        /// Zero-based number of current row in a page, counts all records that are output to client AFTER sorting, filtering and paging.
        /// Exposed in PQL via "rownumoutput()" function.
        /// </summary>
        public long RowNumberInOutput;
        /// <summary>
        /// Input field values.
        /// </summary>
        public DriverRowData InputRow;
        /// <summary>
        /// Values for single-value input parameters.
        /// </summary>
        public DriverRowData InputParametersRow;
        /// <summary>
        /// Values for collection-value input parameters.
        /// </summary>
        public object[] InputParametersCollections;
        /// <summary>
        /// Output field values, only holds entries for fields modified by SET clauses.
        /// </summary>
        public DriverChangeBuffer ChangeBuffer;
    }

    public class ParsedRequest
    {
        public int OrdinalOfPrimaryKey;
        public bool IsBulk;
        public bool HaveParametersDataInput;
        public readonly BufferedReaderStream Bulk;
        public StatementType StatementType;
        public DocumentTypeDescriptor TargetEntity;
        public FieldMetadata TargetEntityPkField;
        public SelectStatementData Select;
        public InsertUpdateDeleteStatementData Modify;
        public BaseDatasetData BaseDataset;
        public BulkInputData BulkInput;
        public ParametersData Params;
        public SpecialCommandData SpecialCommand;

        public ParsedRequest(bool forCache)
        {
            Select = SelectStatementData.Create();
            BaseDataset = BaseDatasetData.Create();
            Modify = InsertUpdateDeleteStatementData.Create();
            BulkInput = BulkInputData.Create();
            Params = ParametersData.Create();
            SpecialCommand = SpecialCommandData.Create();
            
            Bulk = forCache ? null : new BufferedReaderStream(84980);
        }

        public void Clear()
        {
            IsBulk = false;
            HaveParametersDataInput = false;
            OrdinalOfPrimaryKey = -1;
            StatementType = (StatementType)(-1);
            TargetEntity = null;
            TargetEntityPkField = null;
            
            if (Bulk != null)
            {
                Bulk.Detach();
            }

            Select.SelectClauses.Clear();
            Select.SelectFields.Clear();
            Select.OutputColumns.Clear();

            Modify.InsertUpdateSetClauses.Clear();
            Modify.ModifiedFields.Clear();
            Modify.UpdateAssignments.Clear();

            BaseDataset.BaseFieldsMainCount = 0;
            BaseDataset.BaseFields.Clear();
            BaseDataset.WhereClauseFields.Clear();
            BaseDataset.WhereClauseRoot = null;
            BaseDataset.WhereClauseProcessor = null;
            BaseDataset.OrderClauseFields.Clear();
            BaseDataset.OrderClause = null;
            BaseDataset.Paging.Offset = PagingOptions.DefaultPagingOffsetFunc;
            BaseDataset.Paging.PageSize = PagingOptions.DefaultPagingPageSizeFunc;

            BulkInput.BulkInputFields.Clear();

            Params.Names = null;
            Params.InputValues = null;
            Params.InputCollections = null;
            Params.OrdinalToLocalOrdinal = null;
            Params.DataTypes = null;

            SpecialCommand.IsSpecialCommand = false;
            SpecialCommand.CommandType = SpecialCommandData.SpecialCommandType.InvalidValue;
        }

        #region Structures

        /// <summary>
        /// An instance of this structure is created for every single item in SELECT list.
        /// </summary>
        public struct SelectOutputColumn
        {
            /// <summary>
            /// Column label as it is sent to client. May be either a field name or an automatically generated label (for expressions).
            /// </summary>
            public string Label;

            /// <summary>
            /// Compiled functor of some type.
            /// </summary>
            public object CompiledExpression;

            /// <summary>
            /// True if compiled functor returns some flavor of <see cref="UnboxableNullable{T}"/>.
            /// </summary>
            public bool IsNullable;

            /// <summary>
            /// A DbType, derived from compiled functor's return type.
            /// </summary>
            public DbType DbType;
        }

        /// <summary>
        /// An instance of this structure is created for every single SET clause in UPDATE statement,
        /// and for every inserted field in INSERT statement.
        /// </summary>
        public struct FieldAssignment
        {
            /// <summary>
            /// SET which field.
            /// </summary>
            public FieldMetadata Field;
            /// <summary>
            /// Analyzed right-hand side of assignment operator, taken from <see cref="Expression"/>.
            /// </summary>
            public Action<ClauseEvaluationContext> CompiledExpression;
        }

        public struct SelectStatementData
        {
            /// <summary>
            /// List of select elements in the parse tree.
            /// </summary>
            public List<ParseTreeNode> SelectClauses { get; private set; }
            /// <summary>
            /// List of fields used by select expressions (does not include fields in WHERE, ORDER etc.)
            /// </summary>
            public List<FieldMetadata> SelectFields { get; private set; }

            /// <summary>
            /// Ordered list of compiled select expressions with column names, follows order of clauses in SELECT list.
            /// </summary>
            public List<SelectOutputColumn> OutputColumns { get; private set; }

            public static SelectStatementData Create()
            {
                return new SelectStatementData
                {
                    SelectClauses = new List<ParseTreeNode>(),
                    SelectFields = new List<FieldMetadata>(),
                    OutputColumns = new List<SelectOutputColumn>()
                };
            }

            public void WriteTo(ref SelectStatementData dest)
            {
                dest.OutputColumns.Clear();
                foreach (var item in OutputColumns)
                {
                    dest.OutputColumns.Add(item);
                }

                dest.SelectClauses.Clear();
                foreach (var item in SelectClauses)
                {
                    dest.SelectClauses.Add(item);
                }

                dest.SelectFields.Clear();
                foreach(var item in SelectFields)
                {
                    dest.SelectFields.Add(item);
                }
            }
        }

        public struct InsertUpdateDeleteStatementData
        {
            public List<ParseTreeNode> InsertUpdateSetClauses { get; private set; }
            public List<FieldMetadata> ModifiedFields { get; private set; }
            /// <summary>
            /// Ordered list of update assignments, follows order of SET clauses in UPDATE statement.
            /// </summary>
            public List<FieldAssignment> UpdateAssignments { get; private set; }

            public static InsertUpdateDeleteStatementData Create()
            {
                return new InsertUpdateDeleteStatementData
                {
                    InsertUpdateSetClauses = new List<ParseTreeNode>(),
                    ModifiedFields = new List<FieldMetadata>(),
                    UpdateAssignments = new List<FieldAssignment>()
                };
            }

            public void WriteTo(ref InsertUpdateDeleteStatementData dest)
            {
                dest.InsertUpdateSetClauses.Clear();
                foreach (var item in InsertUpdateSetClauses)
                {
                    dest.InsertUpdateSetClauses.Add(item);
                }

                dest.ModifiedFields.Clear();
                foreach (var item in ModifiedFields)
                {
                    dest.ModifiedFields.Add(item);
                }

                dest.UpdateAssignments.Clear();
                foreach (var item in UpdateAssignments)
                {
                    dest.UpdateAssignments.Add(item);
                }
            }
        }

        public struct BulkInputData
        {
            public List<FieldMetadata> BulkInputFields { get; private set; }

            public static BulkInputData Create()
            {
                return new BulkInputData
                    {
                        BulkInputFields = new List<FieldMetadata>()
                    };
            }
        }

        public struct ParametersData
        {
            public string[] Names;
            public DbType[] DataTypes;
            public DriverRowData InputValues;
            public object[] InputCollections;
            public int[] OrdinalToLocalOrdinal;

            public static ParametersData Create() => new ParametersData();
        }

        public struct SpecialCommandData
        {
            public enum SpecialCommandType
            {
                InvalidValue = 0,
                Defragment = 1
            }

            public bool IsSpecialCommand;
            public SpecialCommandType CommandType;

            public static SpecialCommandData Create() => new SpecialCommandData();
        }

        public struct BaseDatasetData
        {
            public List<FieldMetadata> BaseFields { get; private set; }
            public int BaseFieldsMainCount;

            public ParseTreeNode WhereClauseRoot;
            public List<FieldMetadata> WhereClauseFields { get; private set; }
            public Func<ClauseEvaluationContext, bool> WhereClauseProcessor;

            public List<Tuple<int, bool>> OrderClauseFields { get; private set; }
            public ParseTreeNode OrderClause;
            public PagingOptions Paging;

            public static BaseDatasetData Create()
            {
                return new BaseDatasetData
                    {
                        WhereClauseFields = new List<FieldMetadata>(),
                        OrderClauseFields = new List<Tuple<int, bool>>(),
                        BaseFields = new List<FieldMetadata>(),
                        Paging = new PagingOptions
                        {
                            Offset = PagingOptions.DefaultPagingOffsetFunc,
                            PageSize = PagingOptions.DefaultPagingPageSizeFunc
                        }
                    };
            }

            public void WriteTo(ref BaseDatasetData dest)
            {
                dest.BaseFieldsMainCount = BaseFieldsMainCount;
                dest.BaseFields.Clear();
                foreach (var item in BaseFields)
                {
                    dest.BaseFields.Add(item);
                }

                dest.WhereClauseRoot = WhereClauseRoot;
                dest.WhereClauseProcessor = WhereClauseProcessor;
                dest.WhereClauseFields.Clear();
                foreach (var item in WhereClauseFields)
                {
                    dest.WhereClauseFields.Add(item);
                }

                dest.OrderClause = OrderClause;
                dest.OrderClauseFields.Clear();
                foreach (var item in OrderClauseFields)
                {
                    dest.OrderClauseFields.Add(item);
                }

                dest.Paging = Paging;
            }
        }

        public struct PagingOptions
        {
            public static readonly Func<DriverRowData, int> DefaultPagingPageSizeFunc = x => int.MaxValue;
            public static readonly Func<DriverRowData, int> DefaultPagingOffsetFunc = x => 0;

            public Func<DriverRowData, int> PageSize;
            public Func<DriverRowData, int> Offset;
        }
        #endregion
    }
}