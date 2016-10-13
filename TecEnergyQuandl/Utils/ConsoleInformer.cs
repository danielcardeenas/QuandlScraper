using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Utils
{
    public static class ConsoleInformer
    {
        public static void PrintProgress(string taskId, string title, string definition)
        {
            Console.ForegroundColor = ConsoleColor.DarkMagenta;
            Console.Write("{" + taskId + "} ");
            Console.ResetColor();
            Console.Write(title);
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.WriteLine("[" + definition + "]");
            Console.ResetColor();
        }
    }
}
