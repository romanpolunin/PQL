using Pql.ExpressionEngine.Interfaces;

namespace Pql.ExpressionEngine.UnitTest
{
    public static class Extensions
    {
        /// <summary>
        /// A shortcut to yield a Null instance, instantiated for a desired type.
        /// Use some sort of default value of that type to invoke.
        /// Most useful for int, bool etc.
        /// </summary>
        /// <example>0.Null(), or false.Null()</example>
        public static UnboxableNullable<T> Null<T>(this T _) where T : struct => new();
    }
}