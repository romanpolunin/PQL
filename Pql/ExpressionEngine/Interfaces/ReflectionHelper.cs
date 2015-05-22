using System;
using System.Collections.Concurrent;
using System.Reflection;

namespace Pql.ExpressionEngine.Interfaces
{
    /// <summary>
    /// Contains pre-computed reflection data.
    /// </summary>
    public static class ReflectionHelper
    {
        /// <summary>
        /// MethodInfo for Double.IsNaN.
        /// </summary>
        public static readonly MethodInfo DoubleIsNaN = Require(
            typeof(Double), "IsNaN", BindingFlags.Static | BindingFlags.Public, new[] { typeof(Double) });

        /// <summary>
        /// MethodInfo for Double.IsInfinity.
        /// </summary>
        public static readonly MethodInfo DoubleIsInfinity = Require(
            typeof(Double), "IsInfinity", BindingFlags.Static | BindingFlags.Public, new[] { typeof(Double) });

        /// <summary>
        /// MethodInfo for Single.IsNaN.
        /// </summary>
        public static readonly MethodInfo SingleIsNaN = Require(
            typeof(Single), "IsNaN", BindingFlags.Static | BindingFlags.Public, new[] { typeof(Single) });

        /// <summary>
        /// MethodInfo for Single.IsInfinity.
        /// </summary>
        public static readonly MethodInfo SingleIsInfinity = Require(
            typeof(Single), "IsInfinity", BindingFlags.Static | BindingFlags.Public, new[] { typeof(Single) });

        /// <summary>
        /// MethodInfo for String.Concat.
        /// </summary>
        public static readonly MethodInfo StringConcat = Require(
            typeof (string), "Concat", BindingFlags.Static | BindingFlags.Public, new[] {typeof (string), typeof (string)});

        /// <summary>
        /// MethodInfo for String.IsNullOrEmpty.
        /// </summary>
        public static readonly MethodInfo StringIsNullOrEmpty = Require(
            typeof (string), "IsNullOrEmpty", BindingFlags.Static | BindingFlags.Public, new[] {typeof (string) });

        /// <summary>
        /// MethodInfo for String.StartsWith.
        /// </summary>
        public static readonly MethodInfo StringStartsWith = Require(
            typeof(string), "StartsWith", BindingFlags.Instance | BindingFlags.Public, new[] { typeof(string), typeof(StringComparison) });

        /// <summary>
        /// MethodInfo for String.EndsWith.
        /// </summary>
        public static readonly MethodInfo StringEndsWith = Require(
            typeof(string), "EndsWith", BindingFlags.Instance | BindingFlags.Public, new[] { typeof(string), typeof(StringComparison) });

        /// <summary>
        /// MethodInfo for String.IndexOf.
        /// </summary>
        public static readonly MethodInfo StringIndexOf = Require(
            typeof(string), "IndexOf", BindingFlags.Instance | BindingFlags.Public, new[] { typeof(string), typeof(StringComparison) });

        /// <summary>
        /// MethodInfo for StringComparer.Compare.
        /// </summary>
        public static readonly MethodInfo StringComparerCompare = Require(
            typeof (StringComparer), "Compare", BindingFlags.Instance | BindingFlags.Public, new[] {typeof (string), typeof (string)});

        /// <summary>
        /// MethodInfo for StringComparer.Equals.
        /// </summary>
        public static readonly MethodInfo StringComparerEquals = Require(
            typeof (StringComparer), "Equals", BindingFlags.Instance | BindingFlags.Public, new[] {typeof (string), typeof (string)});

        /// <summary>
        /// MethodInfo for ReflectionHelper.EnumerateValues.
        /// </summary>
        public static readonly MethodInfo EnumerateValues = Require(
            typeof (ExpressionTreeExtensions), "EnumerateValues", BindingFlags.Static | BindingFlags.Public, null);

        /// <summary>
        /// MethodInfo for ReflectionHelper.EnumerateValues.
        /// </summary>
        public static readonly MethodInfo DateTimeParseExact = Require(
            typeof (DateTime), "ParseExact", BindingFlags.Static | BindingFlags.Public, new[] {typeof (string), typeof (string), typeof(IFormatProvider)});

        /// <summary>
        /// Reflects on a method, caches found information. Assumes number of arguments being zero.
        /// </summary>
        /// <param name="type">Type to reflect on</param>
        /// <param name="methodName">Public static or instance method to look for</param>
        public static MethodInfo GetOrAddMethodAny(Type type, string methodName)
        {
            return Methods.GetOrAdd(
                new Tuple<Type, string>(type, methodName),
                tuple => Require(type, methodName, BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public, null));
        }

        /// <summary>
        /// Reflects on a method, caches found information. Assumes number of arguments being zero.
        /// </summary>
        /// <param name="type">Type to reflect on</param>
        /// <param name="methodName">Public instance method to look for</param>
        public static MethodInfo GetOrAddMethod0(Type type, string methodName)
        {
            return Methods.GetOrAdd(
                new Tuple<Type, string>(type, methodName),
                tuple => Require(type, methodName, BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public, Type.EmptyTypes));
        }

        /// <summary>
        /// Reflects on a method, caches found information. Assumes number of arguments being one.
        /// </summary>
        /// <param name="type">Type to reflect on</param>
        /// <param name="methodName">Public static or instance method to look for</param>
        /// <param name="type1">Type of the single argument</param>
        public static MethodInfo GetOrAddMethod1(Type type, string methodName, Type type1)
        {
            return Methods.GetOrAdd(
                new Tuple<Type, string>(type, methodName + type1.GetHashCode()), 
                tuple => Require(type, methodName, BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public, new [] {type1}));
        }

        private static MethodInfo Require(Type type, string methodName, BindingFlags bindingFlags, Type[] argTypes)
        {
            var methodInfo = argTypes == null 
                ? type.GetMethod(methodName, bindingFlags)
                : type.GetMethod(methodName, bindingFlags, null, argTypes, null);

            if (methodInfo == null)
            {
                throw new Exception("Failed to reflect a method: " + methodName);
            }

            return methodInfo;
        }

        private readonly static ConcurrentDictionary<Tuple<Type, string>, MethodInfo> Methods = new ConcurrentDictionary<Tuple<Type, string>, MethodInfo>();
    }
}
