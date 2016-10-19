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
            Console.Write("[" + definition + "]");
            Console.WriteLine();
            Console.ResetColor();
        }

        public static void Inform(string definition)
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.Write("[" + definition + "]");
            Console.WriteLine();
            Console.ResetColor();
        }

        public static void InformSimple(string definition)
        {
            Console.ForegroundColor = ConsoleColor.DarkMagenta;
            Console.Write(definition);
            Console.WriteLine();
            Console.ResetColor();
        }

        public static void Result(string definition)
        {
            Console.ForegroundColor = ConsoleColor.DarkBlue;
            Console.Write(definition);
            Console.WriteLine();
            Console.ResetColor();
        }

        public static void Error(string definition)
        {
            Console.ForegroundColor = ConsoleColor.DarkRed;
            Console.Write(definition);
            Console.WriteLine();
            Console.ResetColor();
        }
    }
}
