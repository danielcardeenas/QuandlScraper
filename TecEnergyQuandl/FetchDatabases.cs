using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Threading;
using ShellProgressBar;
using TecEnergyQuandl.Model.ResponseHelpers;
using Newtonsoft.Json.Serialization;
using TecEnergyQuandl.Model;

namespace TecEnergyQuandl
{
    public static class FetchDatabases
    {
        // Init databases list
        public static List<QuandlDatabase> databases = new List<QuandlDatabase>();

        // Stuff to know
        private static int pagesDownloaded = 0;
        private static int pagesToDownload;

        static void Main(string[] args)
        {
            BeginDownloadDatabases();
            Console.ReadLine();
        }
        
        private static void BeginDownloadDatabases()
        {
            using (WebClient client = new WebClient())
            {
                // Make first call to figure out how many pages are there
                // 100 databases or less per page

                // Download first page and check meta
                Console.WriteLine("Fetching databases\n---------------------------------------");
                var databaseReponse = DownloadDatabase(1);

                // To let user knows
                pagesToDownload = databaseReponse.Meta.TotalPages;
                pagesDownloaded++;
                Utils.ConsoleInformer.PrintProgress("1A", "Fetching databases: ", Utils.Helpers.GetPercent(1, databaseReponse.Meta.TotalPages).ToString() + "%");

                // Save first database
                databases.AddRange(databaseReponse.Databases);

                // Download remaining databases
                DownloadDatabasesAsync(2, databaseReponse.Meta.TotalPages);
            }
        }

        private static DatabasesResponse DownloadDatabase(int page)
        {
            using (WebClient client = new WebClient())
            {
                var json = client.DownloadString(Utils.Constants.DATABASES_URL + "?page=" + page + "&api_key=qsoHq8dWs24kyT8pEDSy");
                DatabasesResponse response =
                    JsonConvert.DeserializeObject<DatabasesResponse>(json, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                return response;
            }
        }

        private static void DownloadDatabasesAsync(int fromPage, int toPage)
        {
            foreach(int i in Enumerable.Range(fromPage, toPage - 1))
            {
                DownloadDatabasesAsync(i);
            }
        }

        private static void DownloadDatabasesAsync(int page)
        {
            using (WebClient client = new WebClient())
            {
                client.DownloadStringAsync(new Uri(Utils.Constants.DATABASES_URL + "?page=" + page + "&api_key=qsoHq8dWs24kyT8pEDSy"));
                //client.DownloadProgressChanged += (sender, e) => {};
                client.DownloadStringCompleted += (sender, e) =>
                {
                    DatabasesResponse response =
                        JsonConvert.DeserializeObject<DatabasesResponse>(e.Result, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                    Utils.ConsoleInformer.PrintProgress("1A", "Fetching databases: ", Utils.Helpers.GetPercent(page, response.Meta.TotalPages).ToString() + "%");
                    databases.AddRange(response.Databases);
                    pagesDownloaded++;

                    if (FinishedDownloading())
                    {
                        // Download Datasets
                        Console.WriteLine("");
                        Console.WriteLine("");
                        FetchDatasets.BeginDownloadDatasets();
                    }
                };
            }
        }

        private static bool FinishedDownloading()
        {
            return pagesDownloaded == pagesToDownload;
        } 
    }
}
