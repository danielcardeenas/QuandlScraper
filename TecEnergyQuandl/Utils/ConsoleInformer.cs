using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Utils
{
    public static class ConsoleInformer
    {
        public static void PrintProgress(string taskId, string title, string definition)
        {
            using (var mutex = new Mutex(false, "CONSOLE_PRINT"))
            {
                mutex.WaitOne();
                // Start process
                // ===============================================
                Console.ForegroundColor = ConsoleColor.DarkMagenta;
                Console.Write("{" + taskId + "} ");
                Console.ResetColor();
                Console.Write(title);
                Console.ForegroundColor = ConsoleColor.DarkCyan;
                Console.Write("[" + definition + "]");
                Console.WriteLine();
                Console.ResetColor();

                // End process
                // ===============================================
                mutex.ReleaseMutex();
            }
        }

        public static void PrintProgress(string taskId, string title)
        {
            using (var mutex = new Mutex(false, "CONSOLE_PRINT"))
            {
                mutex.WaitOne();
                // Start process
                // ===============================================
                Console.ForegroundColor = ConsoleColor.DarkMagenta;
                Console.Write("{" + taskId + "} ");
                Console.ResetColor();
                Console.Write(title);
                Console.ForegroundColor = ConsoleColor.DarkCyan;
                Console.WriteLine();
                Console.ResetColor();

                // End process
                // ===============================================
                mutex.ReleaseMutex();
            }
        }

        public static void Inform(string definition)
        {
            using (var mutex = new Mutex(false, "CONSOLE_PRINT"))
            {
                mutex.WaitOne();
                // Start process
                // ===============================================
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.Write("[" + definition + "]");
                Console.WriteLine();
                Console.ResetColor();

                // End process
                // ===============================================
                mutex.ReleaseMutex();
            }
        }

        public static void InformSimple(string definition)
        {
            using (var mutex = new Mutex(false, "CONSOLE_PRINT"))
            {
                mutex.WaitOne();
                // Start process
                // ===============================================
                Console.ForegroundColor = ConsoleColor.DarkMagenta;
                Console.Write(definition);
                Console.WriteLine();
                Console.ResetColor();

                // End process
                // ===============================================
                mutex.ReleaseMutex();
            }
        }

        public static void Result(string definition)
        {
            using (var mutex = new Mutex(false, "CONSOLE_PRINT"))
            {
                mutex.WaitOne();
                // Start process
                // ===============================================
                Console.ForegroundColor = ConsoleColor.DarkBlue;
                Console.Write(definition);
                Console.WriteLine();
                Console.ResetColor();

                // End process
                // ===============================================
                mutex.ReleaseMutex();
            }
        }

        public static void Error(string definition)
        {
            using (var mutex = new Mutex(false, "CONSOLE_PRINT"))
            {
                mutex.WaitOne();
                // Start process
                // ===============================================
                Console.ForegroundColor = ConsoleColor.DarkRed;
                Console.Write(definition);
                Console.WriteLine();
                Console.ResetColor();

                // End process
                // ===============================================
                mutex.ReleaseMutex();
            }
        }
    }
}
