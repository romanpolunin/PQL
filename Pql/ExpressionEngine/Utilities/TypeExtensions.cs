using Pql.ExpressionEngine.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Pql.ExpressionEngine.Utilities
{
    internal static class TypeExtensions
    {
        private static readonly Type[][] m_typeHierarchy = {
            new Type[] { typeof(Byte), typeof(SByte), typeof(Char) },
            new Type[] { typeof(Int16), typeof(UInt16) },
            new Type[] { typeof(Int32), typeof(UInt32) },
            new Type[] { typeof(Int64), typeof(UInt64) },
            new Type[] { typeof(Single) },
            new Type[] { typeof(Double) }
        };

        /// <summary>
        /// Check if type needed to be explicitly casted.
        /// </summary>
        public static bool IsExplicitCastRequired(this Type from, Type to)
        {
            if (ReferenceEquals(from, to))
            {
                return false;
            }

            if (UnboxableNullable.IsNullableType(from) && UnboxableNullable.IsNullableType(to))
            {
                return UnboxableNullable.GetUnderlyingType(from).IsExplicitCastRequired(UnboxableNullable.GetUnderlyingType(to));
            }

            return to.IsAssignableFrom(from) || from.HasCastDefined(to);
        }

        private static bool HasCastDefined(this Type from, Type to)
        {
            if ((from.IsPrimitive || from.IsEnum) && (to.IsPrimitive || to.IsEnum))
            {
                IEnumerable<Type> lowerTypes = Enumerable.Empty<Type>();
                foreach (Type[] types in m_typeHierarchy)
                {
                    if (types.Any(t => t == to))
                    {
                        return lowerTypes.Any(t => t == from);
                    }
                    lowerTypes = lowerTypes.Concat(types);
                }

                return false;
            }
            return IsCastDefined(to, m => m.GetParameters()[0].ParameterType, _ => from, false)
                || IsCastDefined(from, _ => to, m => m.ReturnType, true);
        }

        private static bool IsCastDefined(Type type, Func<MethodInfo, Type> baseType, Func<MethodInfo, Type> derivedType, bool lookInBase)
        {
            var bindinFlags = BindingFlags.Public | BindingFlags.Static
                            | (lookInBase ? BindingFlags.FlattenHierarchy : BindingFlags.DeclaredOnly);

            return type.GetMethods(bindinFlags)
                .Any(
                    m => (m.Name == "op_Implicit" || m.Name == "op_Explicit") && baseType(m).IsAssignableFrom(derivedType(m))
                );
        }
    }
}
