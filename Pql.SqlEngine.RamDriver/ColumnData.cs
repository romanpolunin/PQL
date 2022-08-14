using System.Data;
using System.Linq.Expressions;

using Pql.ExpressionEngine.Interfaces;
using Pql.SqlEngine.Interfaces;
using Pql.SqlEngine.Interfaces.Internal;

namespace Pql.SqlEngine.DataContainer.RamDriver
{
    internal sealed class ColumnData<T> : ColumnDataBase
    {
        private readonly DbType _dbType;
        
        public readonly ExpandableArray<T> DataArray;
        public override DbType DbType => _dbType;

        public ColumnData(DbType dbType, IUnmanagedAllocator allocator)
            : base(allocator)
        {
            _dbType = dbType;
            DataArray = new ExpandableArray<T>(1, typeof(T).IsValueType ? DriverRowData.GetByteCount(dbType) : IntPtr.Size);
            
            AssignFromDriverRow = GenerateAssignFromDriverRowAction();
            AssignToDriverRow = GenerateAssignToDriverRowAction();
            WriteData = GenerateWriteDataAction();
            ReadData = GenerateReadDataAction();
        }

        public ColumnData(ColumnDataBase source, IUnmanagedAllocator allocator)
            : base(source, allocator)
        {
            var typed = (ColumnData<T>) source;

            _dbType = typed.DbType;
            DataArray = typed.DataArray;
            AssignFromDriverRow = typed.AssignFromDriverRow;
            AssignToDriverRow = typed.AssignToDriverRow;
            WriteData = typed.WriteData;
            ReadData = typed.ReadData;
        }

        public override Type ElementType => typeof(T);

        public override bool TryEnsureCapacity(int newCapacity, int timeout = 0)
        {
            var myresult = DataArray.TryEnsureCapacity(newCapacity, timeout);
            var theirresult = base.TryEnsureCapacity(newCapacity, timeout);
            return myresult && theirresult;
        }

        private Action<int, DriverRowData, int> GenerateAssignFromDriverRowAction()
        {
            // NOTE: this method assumes that source value is NOT NULL
            // this should be verified by caller

            var docIndex = Expression.Parameter(typeof(int), "docIndex");
            var rowData = Expression.Parameter(typeof (DriverRowData), "rowData");
            var fieldArrayIndex = Expression.Parameter(typeof (int), "indexInArray");

            var arrayData = Expression.Field(Expression.Constant(this), "DataArray");
            var dataBlock = Expression.Call(arrayData, "GetBlock", null, docIndex);
            var localIndex = Expression.Call(arrayData, "GetLocalIndex", null, docIndex);
            Expression source;
            Expression assign;
            string subPropName;

            var storageType = DriverRowData.DeriveRepresentationType(DbType);
            switch (storageType)
            {
                case DriverRowData.DataTypeRepresentation.ByteArray:
                    // column data may have this destination element uninitialized yet
                    // use Interlocked.CompareExchange when setting its value
                    var target = Expression.Variable(typeof(SizableArrayOfByte), "target");

                    var initIfNull = Expression.IfThen(
                        Expression.ReferenceEqual(Expression.Constant(null), target),
                        Expression.Block(
                            Expression.Assign(target, Expression.New(typeof(SizableArrayOfByte))),
                            Expression.Call(
                                typeof(Interlocked), "CompareExchange", new [] {typeof(SizableArrayOfByte)},
                                Expression.ArrayIndex(dataBlock, localIndex), target, Expression.Constant(null, typeof(SizableArrayOfByte))))
                        );

                    source = Expression.ArrayIndex(Expression.Field(rowData, "BinaryData"), fieldArrayIndex);
                    var copyFrom = typeof (SizableArrayOfByte).GetMethod("CopyFrom", new [] {typeof(SizableArrayOfByte)});
                    var setter = Expression.Call(target, copyFrom, source);
                    
                    assign = Expression.Block(
                        new [] {target},
                        Expression.Assign(target, Expression.ArrayIndex(dataBlock, localIndex)),
                        initIfNull, 
                        setter);
                    break;
                case DriverRowData.DataTypeRepresentation.String:
                    source = Expression.ArrayIndex(Expression.Field(rowData, "StringData"), fieldArrayIndex);
                    assign = Expression.Assign(Expression.ArrayAccess(dataBlock, localIndex), source);
                    break;
                case DriverRowData.DataTypeRepresentation.Value8Bytes:
                    subPropName = DriverRowData.DeriveSystemType(DbType).Name;
                    source = Expression.Field(Expression.ArrayIndex(Expression.Field(rowData, "ValueData8Bytes"), fieldArrayIndex), "As" + subPropName);
                    assign = Expression.Assign(Expression.ArrayAccess(dataBlock, localIndex), source);
                    break;
                case DriverRowData.DataTypeRepresentation.Value16Bytes:
                    subPropName = DriverRowData.DeriveSystemType(DbType).Name;
                    source = Expression.Field(Expression.ArrayIndex(Expression.Field(rowData, "ValueData16Bytes"), fieldArrayIndex), "As" + subPropName);
                    assign = Expression.Assign(Expression.ArrayAccess(dataBlock, localIndex), source);
                    break;
                default:
                    throw new InvalidOperationException("Invalid value for DbType: " + DbType);
            }

            var lambda = Expression.Lambda(
                Expression.GetActionType(new [] {typeof (int), typeof(DriverRowData), typeof(int)}), 
                assign, docIndex, rowData, fieldArrayIndex);

            return (Action<int, DriverRowData, int>)lambda.Compile();
        }

        private Action<int, DriverRowData, int> GenerateAssignToDriverRowAction()
        {
            // NOTE: this method assumes that source value is NOT NULL
            // this should be verified by caller

            var docIndex = Expression.Parameter(typeof (int), "docIndex");
            var rowData = Expression.Parameter(typeof (DriverRowData), "rowData");
            var fieldArrayIndex = Expression.Parameter(typeof (int), "indexInArray");

            var arrayData = Expression.Field(Expression.Constant(this), "DataArray");
            var dataBlock = Expression.Call(arrayData, "GetBlock", null, docIndex);
            var localIndex = Expression.Call(arrayData, "GetLocalIndex", null, docIndex);
            var dataElement = Expression.ArrayIndex(dataBlock, localIndex);

            Expression dest;
            Expression assign;
            string subPropName;

            var storageType = DriverRowData.DeriveRepresentationType(DbType);
            switch (storageType)
            {
                case DriverRowData.DataTypeRepresentation.ByteArray:
                    dest = Expression.ArrayAccess(Expression.Field(rowData, "BinaryData"), fieldArrayIndex);
                    var copyFrom = typeof(SizableArrayOfByte).GetMethod("CopyFrom", new[] { typeof(SizableArrayOfByte) });
                    // we assume that DriverRowData always has destination byte array initialized
                    assign = Expression.Call(dest, copyFrom, dataElement);
                    break;
                case DriverRowData.DataTypeRepresentation.String:
                    dest = Expression.ArrayAccess(Expression.Field(rowData, "StringData"), fieldArrayIndex);
                    assign = Expression.Assign(dest, dataElement);
                    break;
                case DriverRowData.DataTypeRepresentation.Value8Bytes:
                    subPropName = DriverRowData.DeriveSystemType(DbType).Name;
                    dest = Expression.Field(Expression.ArrayAccess(Expression.Field(rowData, "ValueData8Bytes"), fieldArrayIndex), "As" + subPropName);
                    assign = Expression.Assign(dest, dataElement);
                    break;
                case DriverRowData.DataTypeRepresentation.Value16Bytes:
                    subPropName = DriverRowData.DeriveSystemType(DbType).Name;
                    dest = Expression.Field(Expression.ArrayAccess(Expression.Field(rowData, "ValueData16Bytes"), fieldArrayIndex), "As" + subPropName);
                    assign = Expression.Assign(dest, dataElement);
                    break;
                default:
                    throw new InvalidOperationException("Invalid value for DbType: " + DbType);
            }

            var lambda = Expression.Lambda(
                Expression.GetActionType(new[] { typeof(int), typeof(DriverRowData), typeof(int) }),
                assign, docIndex, rowData, fieldArrayIndex);

            return (Action<int, DriverRowData, int>)lambda.Compile();
        }

        private Action<BinaryWriter, int> GenerateWriteDataAction()
        {
            var count = Expression.Parameter(typeof (int), "count");
            var writer = Expression.Parameter(typeof (BinaryWriter), "writer");
            
            var thisref = Expression.Constant(this);
            var docIndex = Expression.Variable(typeof (int), "docIndex");
            var arrayData = Expression.Field(Expression.Constant(this), "DataArray");
            var blockGet = Expression.Call(arrayData, "GetBlock", null, docIndex);
            var block = Expression.Variable(blockGet.Type, "block");
            var dataElement = Expression.ArrayAccess(block, Expression.Call(arrayData, "GetLocalIndex", null, docIndex));

            var notnulls = Expression.Field(thisref, "NotNulls");
            var isnotnull = Expression.Call(notnulls, typeof (BitVector).GetMethod("Get", new [] {typeof(int)}), docIndex);

            var writeItem = Expression.Block(
                Expression.Assign(block, blockGet),
                GenerateWriteItemExpression(typeof (T), dataElement, writer));
            
            var breakLabel = Expression.Label("done");
            var body = Expression.Block(
                new[] { block },
                Expression.IfThen(
                    Expression.GreaterThanOrEqual(docIndex, count), 
                    Expression.Break(breakLabel)),
                Expression.IfThen(isnotnull, writeItem),
                Expression.PreIncrementAssign(docIndex)
                //Expression.Throw(Expression.New(typeof(Exception).GetConstructor(new [] {typeof(string)}), Expression.Call(
                //docIndex, docIndex.Type.GetMethod("ToString", new Type[0]))))
                );

            var loop = Expression.Block(
                new [] {docIndex},
                Expression.Assign(docIndex, Expression.Constant(0, docIndex.Type)),
                Expression.Loop(body, breakLabel))
                ;

            var lambda = Expression.Lambda(
                Expression.GetActionType(new[] { typeof(BinaryWriter), typeof(int) }),
                loop, writer, count);

            return (Action<BinaryWriter, int>) lambda.Compile();
        }

        private Action<BinaryReader, int> GenerateReadDataAction()
        {
            var count = Expression.Parameter(typeof (int), "count");
            var reader = Expression.Parameter(typeof (BinaryReader), "reader");
            
            var thisref = Expression.Constant(this);
            var docIndex = Expression.Variable(typeof (int), "docIndex");
            var arrayData = Expression.Field(Expression.Constant(this), "DataArray");
            var blockGet = Expression.Call(arrayData, "GetBlock", null, docIndex);
            var block = Expression.Variable(blockGet.Type, "block");
            var dataElement = Expression.ArrayAccess(block, Expression.Call(arrayData, "GetLocalIndex", null, docIndex));

            var notnulls = Expression.Field(thisref, "NotNulls");
            var isnotnull = Expression.Call(notnulls, typeof(BitVector).GetMethod("Get", new[] { typeof(int) }), docIndex);

            var readItem = Expression.Block(
                Expression.Assign(block, blockGet),
                GenerateReadItemExpression(typeof (T), dataElement, reader));
            
            var breakLabel = Expression.Label("done");
            var body = Expression.Block(
                new []{block},
                Expression.IfThen(
                    Expression.GreaterThanOrEqual(docIndex, count), 
                    Expression.Break(breakLabel)),
                Expression.IfThen(isnotnull, readItem),
                Expression.PreIncrementAssign(docIndex)
                );

            var loop = Expression.Block(
                new[] {docIndex},
                Expression.Assign(docIndex, Expression.Constant(0, docIndex.Type)),
                Expression.Loop(body, breakLabel))
                ;

            var createDataArray = Expression.Call(arrayData, "EnsureCapacity", null, count);

            var lambda = Expression.Lambda(
                Expression.GetActionType(new[] { typeof(BinaryReader), typeof(int) }),
                Expression.Block(createDataArray, loop), reader, count);

            return (Action<BinaryReader, int>) lambda.Compile();
        }

        private Expression GenerateReadItemExpression(Type itemType, Expression dataElement, ParameterExpression reader)
        {
            if (itemType.IsString())
            {
                var method = reader.Type.GetMethod("ReadString");
                return Expression.Assign(dataElement, Expression.Call(reader, method));
            }

            if (itemType.IsNumeric() || itemType.IsBoolean())
            {
                var method = reader.Type.GetMethod("Read" + itemType.Name);
                return Expression.Assign(dataElement, Expression.Call(reader, method));
            }

            if (itemType.IsDateTime())
            {
                var method = reader.Type.GetMethod("ReadInt64");
                return Expression.Assign(dataElement, Expression.Call(itemType, "FromBinary", null, Expression.Call(reader, method)));
            }

            if (itemType.IsTimeSpan())
            {
                var method = reader.Type.GetMethod("ReadInt64");
                var timespanctr = typeof(TimeSpan).GetConstructor(new[] { typeof(long) });
                return Expression.Assign(dataElement, Expression.New(timespanctr, Expression.Call(reader, method)));
            }

            if (itemType.IsDateTimeOffset())
            {
                var method = reader.Type.GetMethod("ReadInt64");
                var ctr = itemType.GetConstructor(new[] {typeof (DateTime), typeof (TimeSpan)});
                var timespanctr = typeof (TimeSpan).GetConstructor(new[] {typeof (long) });
                return Expression.Assign(dataElement, Expression.New(
                    ctr,
                    Expression.Call(typeof(DateTime), "FromBinary", null, Expression.Call(reader, method)),
                    Expression.New(timespanctr, Expression.Call(reader, method))));
            }

            if (itemType.IsBinary())
            {
                return Expression.Assign(dataElement, Expression.Call(GetType(), "ReadByteArray", null, reader));
            }

            if (itemType.IsGuid())
            {
                return Expression.Assign(dataElement, Expression.Call(GetType(), "ReadGuid", null, reader));
            }

            throw new Exception("Unsupported item type: " + itemType.AssemblyQualifiedName);
        }

        private Expression GenerateWriteItemExpression(Type itemType, Expression dataElement, ParameterExpression writer)
        {
            if (itemType.IsString())
            {
                var method = writer.Type.GetMethod("Write", new[] { itemType });
                return Expression.Call(writer, method, Expression.Coalesce(dataElement, Expression.Constant(string.Empty)));
            }

            if (itemType.IsNumeric() || itemType.IsBoolean())
            {
                var method = writer.Type.GetMethod("Write", new[] { itemType });
                return Expression.Call(writer, method, dataElement);
            }

            if (itemType.IsDateTime())
            {
                var method = writer.Type.GetMethod("Write", new[] { typeof(long) });
                var toBinary = itemType.GetMethod("ToBinary");
                return Expression.Call(writer, method, Expression.Call(dataElement, toBinary));
            }

            if (itemType.IsTimeSpan())
            {
                var method = writer.Type.GetMethod("Write", new[] { typeof(long) });
                return Expression.Call(writer, method, Expression.Property(dataElement, "Ticks"));
            }

            if (itemType.IsDateTimeOffset())
            {
                var method = writer.Type.GetMethod("Write", new[] { typeof(long) });
                var toBinary = typeof(DateTime).GetMethod("ToBinary");
                return Expression.Block(
                    Expression.Call(writer, method, Expression.Call(Expression.Property(dataElement, "DateTime"), toBinary)),
                    Expression.Call(writer, method, Expression.PropertyOrField(Expression.Property(dataElement, "Offset"), "Ticks")));
            }

            if (itemType.IsBinary())
            {
                var method = writer.Type.GetMethod("Write", new[] { typeof(byte[]), typeof(int), typeof(int) });

                var buffer = Expression.Field(dataElement, "Data");
                var length = Expression.Field(dataElement, "Length");
                return Expression.Block(
                    Expression.Call(GetType(), "Write7BitEncodedInt", null, writer, length),
                    Expression.IfThen(
                        Expression.GreaterThan(length, Expression.Constant(0)),
                        Expression.Call(writer, method, buffer, Expression.Constant(0), length))
                    );
            }

            if (itemType.IsGuid())
            {
                var buf = Expression.Variable(typeof(DriverRowData.ValueHolder16Bytes), "buf");
                var method = writer.Type.GetMethod("Write", new[] { typeof(long) });
                return Expression.Block(
                    new [] {buf},
                    Expression.Assign(Expression.Field(buf, "AsGuid"), dataElement),
                    Expression.Call(writer, method, Expression.Field(buf, "Lo")),
                    Expression.Call(writer, method, Expression.Field(buf, "Hi")));
            }

            throw new Exception("Unsupported item type: " + itemType.AssemblyQualifiedName);
        }
    }
}