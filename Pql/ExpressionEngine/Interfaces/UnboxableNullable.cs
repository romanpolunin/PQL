using Pql.ExpressionEngine.Utilities;
using System;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// Replaces .NET nullable. 
    /// Main purpose is to avoid boxing it as null reference, which is a big problem for expression compilation. 
    /// Another purpose is to incorporate some custom behavior and get rid of all kinds of operator overloads defined on .NET Nullable.
    /// </summary>
    public struct UnboxableNullable<T> : IConvertible
        where T: struct
    {
        /// <summary>
        /// Value.
        /// </summary>
        public readonly T Value;

        /// <summary>
        /// True if value is initialized.
        /// </summary>
        public readonly bool HasValue;

        /// <summary>
        /// Ctr.
        /// </summary>
        public UnboxableNullable(T value)
        {
            Value = value;
            HasValue = true;
        }

        /// <summary>
        /// Returns value or default(T) if value is not set.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetValueOrDefault()
        {
            return HasValue ? Value : default(T);
        }

        /// <summary>
        /// Indicates whether this instance and a specified object are equal.
        /// </summary>
        /// <returns>
        /// true if <paramref name="obj"/> and this instance are the same type and represent the same value; otherwise, false.
        /// </returns>
        /// <param name="obj">Another object to compare to. </param><filterpriority>2</filterpriority>
        public override bool Equals(object obj)
        {
            var other = (UnboxableNullable<T>)obj;
            return HasValue
                       ? other.HasValue && other.Value.Equals(Value)
                       : !other.HasValue;
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>
        /// A 32-bit signed integer that is the hash code for this instance.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public TypeCode GetTypeCode()
        {
            return TypeCode.Object;
        }

        public object ToType(Type conversionType, IFormatProvider provider)
        {
            bool isCastable = typeof(UnboxableNullable<T>).IsExplicitCastRequired(conversionType);

            if (!UnboxableNullable.IsNullableType(conversionType) || (HasValue && !isCastable))
            {
                throw new InvalidCastException();
            }

            Type genericType = typeof(UnboxableNullable<>).MakeGenericType(conversionType.GetUnderlyingType());

            if (HasValue)
            {
                return Activator.CreateInstance(genericType, Convert.ChangeType(Value, conversionType.GetUnderlyingType()));
            }

            return Activator.CreateInstance(genericType);
        }

        #region IConvertible invalid casts

        public bool ToBoolean(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public byte ToByte(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public char ToChar(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public DateTime ToDateTime(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public decimal ToDecimal(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public double ToDouble(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public short ToInt16(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public int ToInt32(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public long ToInt64(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public sbyte ToSByte(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public float ToSingle(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public string ToString(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public ushort ToUInt16(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public uint ToUInt32(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        public ulong ToUInt64(IFormatProvider provider)
        {
            throw new InvalidCastException();
        }

        #endregion

        #region Overloaded operators

        /// <summary>
        /// Overloaded equality comparison.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator == (UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return x.HasValue
                       ? y.HasValue && y.Value.Equals(x.Value)
                       : !y.HasValue;
        }

        /// <summary>
        /// Overloaded equality comparison.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator != (UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return x.HasValue
                       ? !y.HasValue || !y.Value.Equals(x.Value)
                       : y.HasValue;
        }

        /// <summary>
        /// Overloaded equality comparison.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator >(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return x.HasValue && y.HasValue
                && ConstantHelper.InvokeBinaryOperation<T, bool>(ExpressionType.GreaterThan, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded equality comparison.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator <(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return x.HasValue && y.HasValue
                && ConstantHelper.InvokeBinaryOperation<T, bool>(ExpressionType.LessThan, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded equality comparison.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator >=(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return x.HasValue && y.HasValue
                && ConstantHelper.InvokeBinaryOperation<T, bool>(ExpressionType.GreaterThanOrEqual, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded equality comparison.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator <=(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return x.HasValue && y.HasValue
                && ConstantHelper.InvokeBinaryOperation<T, bool>(ExpressionType.LessThanOrEqual, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator +(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Add, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator -(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Subtract, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator *(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Multiply, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator /(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Divide, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator ^(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.ExclusiveOr, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator %(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Modulo, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator &(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.And, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator |(UnboxableNullable<T> x, UnboxableNullable<T> y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Or, x.Value, y.Value);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator +(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Add, x.Value, y);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator -(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Subtract, x.Value, y);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator *(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Multiply, x.Value, y);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator /(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Divide, x.Value, y);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator ^(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.ExclusiveOr, x.Value, y);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator %(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Modulo, x.Value, y);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator &(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.And, x.Value, y);
        }

        /// <summary>
        /// Overloaded operator.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static T operator |(UnboxableNullable<T> x, T y)
        {
            return ConstantHelper.InvokeBinaryOperation(ExpressionType.Or, x.Value, y);
        }

        #endregion

        /// <summary>
        /// Default conversion.
        /// </summary>
        public static implicit operator UnboxableNullable<T>(T value)
        {
            return new UnboxableNullable<T>(value);
        }
    }

    /// <summary>
    /// Utilities for <see cref="UnboxableNullable{T}"/>.
    /// </summary>
    public static class UnboxableNullable
    {
        /// <summary>
        /// Retrieves underlying value type.
        /// </summary>
        public static Type GetUnderlyingType(Type nullableType)
        {
            if (nullableType == null)
            {
                throw new ArgumentNullException("nullableType");
            }

            return IsNullableType(nullableType)
                ? nullableType.GetGenericArguments()[0]
                : null;
        }

        /// <summary>
        /// Checks if type is UnboxableNullable<>.
        /// </summary>
        public static bool IsNullableType(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            return type.IsGenericType && !type.IsGenericTypeDefinition && ReferenceEquals(type.GetGenericTypeDefinition(), typeof(UnboxableNullable<>));
        }

        /// <summary>
        /// Default conversion.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static UnboxableNullable<T> Null<T>(this T obj) where T : struct
        {
            return new UnboxableNullable<T>();
        }
    }
}