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
            try { MainAsync().Wait(); }
            catch (Exception ex) { Utils.Helpers.ExitWithError(ex.InnerException.Message); }

            // Finish
            Console.ReadLine();
        }

        // The program consists in 3 main steps
        // You can comment whichever you want and program should still run smoothly
        private static async Task MainAsync()
        {
            // 1. Download databases available
            //await BeginDownloadDatabases();

            // 2. Download datasets 
            //  Only the ones selected in quandl.databases (import = true)
            await BeginDownloadDatasets();

            // 3. Download datasets data
            //  Only from the datasets selected in quandl.datasets (import = true)
            //await BeginDownloadDatasetsData();
        }

        public static async Task BeginDownloadDatabases()
        {
            // Only needed first run
            // Or if you want to reset quandl databases
            PostgresHelpers.SchemaActions.MakeDatabase();
            Console.WriteLine("\n");

            await FetchDatabases.BeginDownloadDatabases();
            Console.WriteLine("\n");

            Console.WriteLine("############################################################################");
            Console.WriteLine("Program is paused, now you should select the Quandl Databases in Postgress. \nPress enter to continue...");
            Console.WriteLine("############################################################################");
            Console.ReadLine();
        }

        public static async Task BeginDownloadDatasets()
        {
            await FetchDatasets.BeginDownloadDatasets();
            Console.WriteLine("\n");

            Console.WriteLine("############################################################################");
            Console.WriteLine("Program is paused, now you should select the Quandl Datasets in Postgress. \nPress enter to continue...");
            Console.WriteLine("############################################################################");
            Console.ReadLine();
        }

        public static async Task BeginDownloadDatasetsData()
        {
            await FetchData.BeginDownloadData();
            Console.WriteLine("\n");

            Console.WriteLine("############################################################################");
            Console.WriteLine("Finished fetching data. \nPress enter to exit...");
            Console.WriteLine("############################################################################");
        }
    }
}
