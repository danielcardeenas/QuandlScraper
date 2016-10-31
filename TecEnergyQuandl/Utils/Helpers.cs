using System;
using System.Collections.Generic;
using System.IO;
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
            ConsoleInformer.Error("Press enter to exit...");
            Console.ReadLine();
            Environment.Exit(1);
        }

        public static void Log(string logMessage, string info, StreamWriter txtWriter)
        {
            txtWriter.WriteLine("[{0} || {1}]: {2}. {3}", DateTime.Now.ToLongTimeString(),
                DateTime.Now.ToLongDateString(), logMessage, info);
        }
    }
}
