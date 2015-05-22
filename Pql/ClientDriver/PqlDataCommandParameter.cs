using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Reflection;

namespace Pql.ClientDriver
{
    /// <summary>
    /// Implements a holder for parameter data.
    /// </summary>
    public class PqlDataCommandParameter : DbParameter, IDbDataParameter
    {
        /// <summary>
        /// A call to <see cref="Validate"/> sets this property if client supplied a valid collection in <see cref="Value"/>.
        /// </summary>
        internal bool IsValidatedCollection { get; private set; }

        /// <summary>
        /// Resets the DbType property to its original settings.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public override void ResetDbType()
        {
            
        }

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.DbType"/> of the parameter.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.DbType"/> values. The default is <see cref="F:System.Data.DbType.String"/>.
        /// </returns>
        /// <exception cref="T:System.ArgumentOutOfRangeException">The property was not set to a valid <see cref="T:System.Data.DbType"/>. </exception><filterpriority>2</filterpriority>
        public override DbType DbType { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.ParameterDirection"/> values. The default is Input.
        /// </returns>
        /// <exception cref="T:System.ArgumentException">The property was not set to one of the valid <see cref="T:System.Data.ParameterDirection"/> values. </exception><filterpriority>2</filterpriority>
        public override ParameterDirection Direction
        {
            get { return ParameterDirection.Input; }
            set { }
        }

        /// <summary>
        /// Gets a value indicating whether the parameter accepts null values.
        /// </summary>
        /// <returns>
        /// true if null values are accepted; otherwise, false. The default is false.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override bool IsNullable
        {
            get { return true; } set { }
        }

        /// <summary>
        /// Gets or sets the name of the <see cref="T:System.Data.IDataParameter"/>.
        /// </summary>
        /// <returns>
        /// The name of the <see cref="T:System.Data.IDataParameter"/>. The default is an empty string.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ParameterName { get; set; }

        /// <summary>
        /// Gets or sets the name of the source column that is mapped to the <see cref="T:System.Data.DataSet"/> and used for loading or returning the <see cref="P:System.Data.IDataParameter.Value"/>.
        /// </summary>
        /// <returns>
        /// The name of the source column that is mapped to the <see cref="T:System.Data.DataSet"/>. The default is an empty string.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string SourceColumn
        {
            get { return null; }
            set { }
        }

        /// <summary>
        /// Sets or gets a value which indicates whether the source column is nullable. 
        /// This allows <see cref="T:System.Data.Common.DbCommandBuilder"/> to correctly generate Update statements for nullable columns.
        /// </summary>
        /// <returns>
        /// true if the source column is nullable; false if it is not.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool SourceColumnNullMapping { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="T:System.Data.DataRowVersion"/> to use when loading <see cref="P:System.Data.IDataParameter.Value"/>.
        /// </summary>
        /// <returns>
        /// One of the <see cref="T:System.Data.DataRowVersion"/> values. The default is Current.
        /// </returns>
        /// <exception cref="T:System.ArgumentException">The property was not set one of the <see cref="T:System.Data.DataRowVersion"/> values. </exception>
        /// <filterpriority>2</filterpriority>
        public override DataRowVersion SourceVersion
        {
            get { return DataRowVersion.Default; }
            set { }
        }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Object"/> that is the value of the parameter. The default value is null.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override object Value { get; set; }

        /// <summary>
        /// Indicates the precision of numeric parameters.
        /// </summary>
        /// <returns>
        /// The maximum number of digits used to represent the Value property of a data provider Parameter object. 
        /// The default value is 0, which indicates that a data provider sets the precision for Value.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override byte Precision { get { return 0; } set { } }

        /// <summary>
        /// Indicates the scale of numeric parameters.
        /// </summary>
        /// <returns>
        /// The number of decimal places to which <see cref="Value"/> is resolved. The default is 0.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override byte Scale { get { return 0; } set { } }

        /// <summary>
        /// The size of the parameter.
        /// </summary>
        /// <returns>
        /// The maximum size, in bytes, of the data within the column. The default value is inferred from the the parameter value.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int Size { get { return 0; } set { } }

        /// <summary>
        /// Validates consistency of configuration values on this parameter instance.
        /// </summary>
        public void Validate()
        {
            IsValidatedCollection = false;

            if (Direction != ParameterDirection.Input)
            {
                throw new DataException("Direction must be set to " + ParameterDirection.Input);
            }

            if (string.IsNullOrEmpty(ParameterName))
            {
                throw new DataException("ParameterName is null or empty");
            }

            if (ParameterName.Length < 2 || ParameterName[0] != '@')
            {
                throw new DataException("ParameterName must have at least two characters and start with '@'");
            }

            for (var index = 1; index < ParameterName.Length; index++)
            {
                var c = ParameterName[index];
                if (!Char.IsLetterOrDigit(c))
                {
                    throw new DataException("After initial '@', ParameterName can only have letters and digits: " + ParameterName);
                }
            }

            // validate DbType enum
            var expectedType = RowData.DeriveSystemType(DbType);

            // perform basic sanity check for most collection and value types
            if (Value != null && Value != DBNull.Value)
            {
                var vtype = Value.GetType();

                if (DbType == DbType.Binary || DbType == DbType.Object || vtype.IsValueType)
                {
                    
                }
                else if (vtype.IsArray)
                {
                    if (((Array) Value).Rank != 1)
                    {
                        throw new DataException("Parameter value cannot be a multidimensional array");
                    }
                    vtype = vtype.GetElementType();
                    IsValidatedCollection = true;
                }
                else if (vtype != typeof(string))
                {
                    // check if Value is of supported collection type
                    var interfaces = vtype.GetInterfaces();
                    foreach (var intf in interfaces)
                    {
                        if (intf.IsGenericType)
                        {
                            var basetype = intf.GetGenericTypeDefinition();
                            if (basetype == typeof(ICollection<>) || basetype == typeof(IReadOnlyCollection<>))
                            {
                                vtype = intf.GetGenericArguments()[0];
                                if (vtype == expectedType)
                                {
                                    IsValidatedCollection = true;
                                    break;
                                }
                            }
                        }
                    }

                    // do not support basic ICollection in order to avoid unboxing when writing values to request stream
                    // boxing-unboxing can seriously affect client performance on large value sets
                    if (!IsValidatedCollection && Value is ICollection)
                    {
                        throw new DataException(
                                "To provide a collection of values as a parameter's value, use strongly typed 1-dimensional arrays"
                                + " or generic collections implementing ICollection<T>, IReadOnlyCollection<T>.");
                    }
                }
                
                if (vtype != expectedType)
                {
                    throw new DataException(
                        string.Format(
                            "Value contains an instance of type {0}, which is different from what is implied by DbType ({1}): {2}",
                            vtype.FullName, DbType, expectedType.FullName));
                }
            }
        }

        /// <summary>
        /// Writes value of this parameter to stream.
        /// </summary>
        internal void Write(BinaryWriter writer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException("writer");
            }

            if (Value == null || Value == DBNull.Value)
            {
                throw new InvalidOperationException("Write should not be called on parameters with null value");
            }

            if (IsValidatedCollection)
            {
                var count = GetCollectionCount(Value);
                RowData.Write7BitEncodedInt(writer, count);

                if (count > 0)
                {
                    var enumerator = GetEnumerator(Value);
                    var enumwriter = GetEnumeratorWriter(DbType);

                    enumwriter(writer, enumerator);
                }
            }
            else
            {
                WritePrimitiveValue(writer, DbType, Value);
            }
        }

        private static void WritePrimitiveValue(BinaryWriter writer, DbType dbType, object value)
        {
            switch (dbType)
            {
                    //case DbType.VarNumeric:
                    //    break;
                case DbType.AnsiString:
                case DbType.String:
                case DbType.AnsiStringFixedLength:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    {
                        var data = (string) value;
                        var len = data.Length; // we don't expect null values here
                        RowData.Write7BitEncodedInt(writer, len);
                        for (var i = 0; i < len; i++)
                        {
                            RowData.Write7BitEncodedInt(writer, data[i]);
                        }
                    }
                    break;

                case DbType.Binary:
                case DbType.Object:
                    {
                        var data = (byte[]) value;
                        var len = data.Length; // we don't expect null values here
                        RowData.Write7BitEncodedInt(writer, len);
                        if (len > 0)
                        {
                            writer.Write(data, 0, data.Length);
                        }
                    }
                    break;

                case DbType.Byte:
                    writer.Write((Byte) value);
                    break;

                case DbType.Boolean:
                    writer.Write((bool) value);
                    break;

                case DbType.Time:
                    writer.Write(((TimeSpan)value).Ticks);
                    break;

                case DbType.Date:
                case DbType.DateTime2:
                case DbType.DateTime:
                    writer.Write(((DateTime) value).ToBinary());
                    break;

                case DbType.Currency:
                case DbType.Decimal:
                    writer.Write((Decimal) value);
                    break;

                case DbType.Double:
                    writer.Write((Double) value);
                    break;

                case DbType.Guid:
                    {
                        var curr = new RowData.ValueHolder16Bytes {AsGuid = (Guid) value};
                        writer.Write(curr.Lo);
                        writer.Write(curr.Hi);
                    }
                    break;

                case DbType.Int16:
                    writer.Write((Int16) value);
                    break;

                case DbType.Int32:
                    writer.Write((Int32) value);
                    break;

                case DbType.Int64:
                    writer.Write((Int64) value);
                    break;

                case DbType.SByte:
                    writer.Write((SByte) value);
                    break;

                case DbType.Single:
                    writer.Write((Single)value);
                    break;

                case DbType.UInt16:
                    writer.Write((UInt16) value);
                    break;

                case DbType.UInt32:
                    writer.Write((UInt32) value);
                    break;

                case DbType.UInt64:
                    writer.Write((UInt64) value);
                    break;


                case DbType.DateTimeOffset:
                    {
                        var curr = new RowData.ValueHolder16Bytes { AsDateTimeOffset = (DateTimeOffset)value };
                        writer.Write(curr.Lo);
                        writer.Write(curr.Hi);
                    }
                    break;

                default:
                    throw new DataException("Invalid DbType: " + dbType);
            }
        }

        private static Action<BinaryWriter, object> GetEnumeratorWriter(DbType dbType)
        {
            switch (dbType)
            {
                //case DbType.VarNumeric:
                //    break;
                case DbType.AnsiString:
                case DbType.String:
                case DbType.AnsiStringFixedLength:
                case DbType.StringFixedLength:
                case DbType.Xml:
                    return (writer, untyped) =>
                        {
                            var typed = (IEnumerator<string>) untyped;
                            while (typed.MoveNext())
                            {
                                var data = typed.Current;
                                var len = data == null ? -1 : data.Length;
                                RowData.Write7BitEncodedInt(writer, len);
                                for (var i = 0; i < len; i++)
                                {
                                    RowData.Write7BitEncodedInt(writer, data[i]);
                                }
                            }
                        };
                    
                case DbType.Binary:
                case DbType.Object:
                    return (writer, untyped) =>
                        {
                            var typed = (IEnumerator<byte[]>) untyped;
                            while (typed.MoveNext())
                            {
                                var data = typed.Current;
                                var len = data == null ? -1 : data.Length;
                                RowData.Write7BitEncodedInt(writer, len);
                                if (len > 0)
                                {
                                    writer.Write(data, 0, data.Length);
                                }
                            }
                        };
                    
                case DbType.Byte:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Byte>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.Boolean:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Boolean>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.Time:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<TimeSpan>) untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current.Ticks);
                        }
                    };

                case DbType.Date:
                case DbType.DateTime2:
                case DbType.DateTime:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<DateTime>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current.ToBinary());
                        }
                    };

                case DbType.Currency:
                case DbType.Decimal:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Decimal>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.Double:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Double>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.Guid:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Guid>)untyped;
                        var curr = new RowData.ValueHolder16Bytes();
                        while (typed.MoveNext())
                        {
                            curr.AsGuid = typed.Current;
                            writer.Write(curr.Lo);
                            writer.Write(curr.Hi);
                        }
                    };

                case DbType.Int16:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Int16>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.Int32:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Int32>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.Int64:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Int64>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.SByte:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<SByte>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.Single:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<Single>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.UInt16:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<UInt16>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.UInt32:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<UInt32>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.UInt64:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<UInt64>)untyped;
                        while (typed.MoveNext())
                        {
                            writer.Write(typed.Current);
                        }
                    };

                case DbType.DateTimeOffset:
                    return (writer, untyped) =>
                    {
                        var typed = (IEnumerator<DateTimeOffset>)untyped;
                        var curr = new RowData.ValueHolder16Bytes();
                        while (typed.MoveNext())
                        {
                            curr.AsDateTimeOffset = typed.Current;
                            writer.Write(curr.Lo);
                            writer.Write(curr.Hi);
                        }
                    };
                default:
                    throw new DataException("Invalid DbType: " + dbType);
            }
        }

        private static object GetEnumerator(object value)
        {
            // all accepted collection types implement IEnumerable of T (typed arrays, collections and read-only collections)
            var vtype = value.GetType();
            var enumerable = typeof(IEnumerable<>).MakeGenericType(vtype.GetElementType());
            var method = enumerable.GetMethod("GetEnumerator", new Type[0]);
            try
            {
                return method.Invoke(value, null);
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException;
            }
        }

        private static int GetCollectionCount(object value)
        {
            var vtype = value.GetType();
            var prop = vtype.GetProperty(vtype.IsArray ? "Length" : "Count");
            return (int) prop.GetValue(value);
        }
    }
}