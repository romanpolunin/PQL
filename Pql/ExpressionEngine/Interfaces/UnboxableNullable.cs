﻿using System;
using System.Runtime.CompilerServices;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// Replaces .NET nullable. 
    /// Main purpose is to avoid boxing it as null reference, which is a big problem for expression compilation. 
    /// Another purpose is to incorporate some custom behavior and get rid of all kinds of operator overloads defined on .NET Nullable.
    /// </summary>
    public struct UnboxableNullable<T>
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
            return nullableType.IsGenericType && !nullableType.IsGenericTypeDefinition
                   && ReferenceEquals(nullableType.GetGenericTypeDefinition(), typeof(UnboxableNullable<>))
                       ? nullableType.GetGenericArguments()[0]
                       : null;
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