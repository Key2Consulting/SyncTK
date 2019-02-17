using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncTK
{
    internal static class Util
    {
        public static string IIF(bool condition, string truePart, string falsePart = "")
        {
            if (condition)
                return truePart;
            else
                return falsePart;
        }

        // https://stackoverflow.com/questions/108104/how-do-i-convert-a-system-type-to-its-nullable-version
        static public Type GetNullableType(Type TypeToConvert)
        {
            // Abort if no type supplied
            if (TypeToConvert == null)
                return null;

            // If the given type is already nullable, just return it
            if (IsTypeNullable(TypeToConvert))
                return TypeToConvert;

            // If the type is a ValueType and is not System.Void, convert it to a Nullable<Type>
            if (TypeToConvert.IsValueType && TypeToConvert != typeof(void))
                return typeof(Nullable<>).MakeGenericType(TypeToConvert);

            // Done - no conversion
            return null;
        }

        static public bool IsTypeNullable(Type TypeToTest)
        {
            // Abort if no type supplied
            if (TypeToTest == null)
                return false;

            // If this is not a value type, it is a reference type, so it is automatically nullable
            //  (NOTE: All forms of Nullable<T> are value types)
            if (!TypeToTest.IsValueType)
                return true;

            // Report whether TypeToTest is a form of the Nullable<> type
            return TypeToTest.IsGenericType && TypeToTest.GetGenericTypeDefinition() == typeof(Nullable<>);
        }
    }
}
