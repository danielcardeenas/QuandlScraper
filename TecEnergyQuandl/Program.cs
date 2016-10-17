using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl
{
    public static class Program
    {
        static void Main(string[] args)
        {
            // Only needed first run
            // Or if you want to reset quandl databases
            PostgresHelpers.SchemaActions.CreateSchema();
            Console.WriteLine("\n");

            MainAsync().Wait();

            // Finish
            Console.ReadLine();
        }

        private static async Task MainAsync()
        {
            // Download databases
            await BeginDownloadDatabases();

            // Download Datasets
            FetchDatasets.BeginDownloadDatasets();
        }

        public static async Task BeginDownloadDatabases()
        {
            await FetchDatabases.BeginDownloadDatabases();
            Console.WriteLine("\n");
        }

        public static void BeginDownloadDatasets()
        {
            FetchDatasets.BeginDownloadDatasets();
            Console.WriteLine("\n");
        }
    }
}
