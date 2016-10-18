using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;
using TecEnergyQuandl.Model.ResponseHelpers;

namespace TecEnergyQuandl
{
    public static class FetchDatasets
    {
        private static List<QuandlDatabase> databases;
        private static List<QuandlDataset> datasets = new List<QuandlDataset>();

        private static int pagesSum;
        private static bool firstTime = true;
        public static async Task BeginDownloadDatasets()
        {
            // Download first page and check meta
            Console.WriteLine("Fetching datasets\n---------------------------------------");
            databases = PostgresHelpers.QuandlDatabaseActions.GetImportedDatabases();

            Console.WriteLine("\nSelected databases:");
            databases.ForEach(db =>
                Console.WriteLine(" -[DB] " + db.Name + " - " + db.DatabaseCode)
            );
            Console.WriteLine();

            foreach (QuandlDatabase database in databases)
            {
                // Starting 0%
                pagesSum = 0;
                Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets for [" + database.DatabaseCode + "]: ", "0%");

                // Get first datasets page ordered
                var datasetsReponse = DownloadDataset(1, database);

                // Download remaining datasets
                if (datasetsReponse.Meta.TotalPages >= 2)
                    await DownloadDatasetsAsync(2, datasetsReponse.Meta.TotalPages, database);

                Utils.ConsoleInformer.Inform("#############################");
                Utils.ConsoleInformer.Inform(" -[DB] " + database.DatabaseCode + " Finished download");
                Utils.ConsoleInformer.Inform("#############################");
            }
        }

        private static DatasetsResponse DownloadDataset(int page, QuandlDatabase database)
        {
            using (WebClient client = new WebClient())
            {
                var json = client.DownloadString("https://www.quandl.com/api/v3/datasets.json?database_code=" + database.DatabaseCode + "&sort_by=id&page=" + page + "&api_key=" + Utils.Constants.API_KEY);
                DatasetsResponse response =
                    JsonConvert.DeserializeObject<DatasetsResponse>(json, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                pagesSum++;
                Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets for [" + database.DatabaseCode + "]: ", Utils.Helpers.GetPercent(pagesSum, response.Meta.TotalPages).ToString() + "%");

                // Add first page results
                datasets.AddRange(response.Datasets);

                return response;
            }
        }

        private static async Task DownloadDatasetsAsync(int fromPage, int toPage, QuandlDatabase database)
        {
            // Fix console cursor position
            if (firstTime) { HotFixConsoleCursor(); }

            var pages = Enumerable.Range(fromPage, toPage - 1);
            await Task.WhenAll(pages.Select(i => DownloadDatasetsAsync(i, database)));

            //Console.WriteLine(" -[DB] " + database.DatabaseCode + " Finished download");
        }

        private static async Task DownloadDatasetsAsync(int page, QuandlDatabase database)
        {
            using (WebClient client = new WebClient())
            {
                string data = await client.DownloadStringTaskAsync(new Uri("https://www.quandl.com/api/v3/datasets.json?database_code=" + database.DatabaseCode + "&sort_by=id&page=" + page + "&api_key=" + Utils.Constants.API_KEY));
                DatasetsResponse response =
                        JsonConvert.DeserializeObject<DatasetsResponse>(data, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                pagesSum++;
                Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets for [" + database.DatabaseCode + "]: ", Utils.Helpers.GetPercent(pagesSum, response.Meta.TotalPages).ToString() + "%");
                datasets.AddRange(response.Datasets);
            }
        }

        private static void HotFixConsoleCursor()
        {
            Console.CursorTop--;
            Console.CursorTop--;
            firstTime = false;
        }
    }
}
