using System;
using System.Reflection;
using Irony.Parsing;

namespace Pql.ExpressionEngine.Utilities
{
    public static class IronyExtensionMethods
    {
        private static readonly MethodInfo s_method;

        static IronyExtensionMethods()
        {
            var t = typeof(Parser);
            s_method = t.GetMethod("Reset", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            if (s_method is null)
            {
                throw new Exception("Could not resolve internal method Reset on " + t.FullName);
            }
        }

        public static void Reset(this Parser parser)
        {
            s_method.Invoke(parser, null);
        }
    }
}