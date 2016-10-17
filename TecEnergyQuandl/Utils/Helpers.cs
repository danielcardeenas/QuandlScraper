using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Utils
{
    public static class Helpers
    {
        public static double GetPercent(int value, int of)
        {
            return Math.Ceiling((double)(value * 100) / of);
        }

        public static void ExitWithError(string error)
        {
            ConsoleInformer.Error("Error: " + error);
            Console.ReadLine();
            Environment.Exit(1);
        }
    }
}
