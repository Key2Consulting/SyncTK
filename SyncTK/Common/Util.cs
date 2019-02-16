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
    }
}
