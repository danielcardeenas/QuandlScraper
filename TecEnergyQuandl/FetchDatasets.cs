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
        private static List<QuandlDatasetGroup> datasetsGroups = new List<QuandlDatasetGroup>();

        private static int pagesSum;
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

            int count = 0;
            foreach (QuandlDatabase database in databases)
            {
                // Starting 0%
                pagesSum = 0;
                count++;

                // Each database -> gives a bunch of datasets of its own kind
                // So this is called a new group
                datasetsGroups.Add(new QuandlDatasetGroup() { DatabaseCode = database.DatabaseCode, Datasets = new List<QuandlDataset>() });

                //Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets [" + database.DatabaseCode + "]: ", "0%");

                // Get first datasets page ordered
                var datasetsReponse = DownloadDataset(1, database);

                // Download remaining datasets
                if (datasetsReponse.Meta.TotalPages >= 2)
                    await DownloadDatasetsAsync(2, datasetsReponse.Meta.TotalPages, database);

                Utils.ConsoleInformer.InformSimple("[DB] " + database.DatabaseCode + " Done. [" + count + "/" + databases.Count + "]");
                Utils.ConsoleInformer.InformSimple("-------------------------------------");
            }

            // Manipulate data into database
            Console.WriteLine("\nInserting data into database\n---------------------------------------");

            // Make datasets list
            PostgresHelpers.QuandlDatasetActions.InsertQuandlDatasets(datasetsGroups);
        }

        private static DatasetsResponse DownloadDataset(int page, QuandlDatabase database)
        {
            using (WebClient client = new WebClient())
            {
                try {
                    var json = client.DownloadString("https://www.quandl.com/api/v3/datasets.json?database_code=" + database.DatabaseCode + "&sort_by=id&page=" + page + "&api_key=" + Utils.Constants.API_KEY);
                    DatasetsResponse response =
                        JsonConvert.DeserializeObject<DatasetsResponse>(json, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                    pagesSum++;
                    Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets [" + database.DatabaseCode + "]: ", Utils.Helpers.GetPercent(pagesSum, response.Meta.TotalPages).ToString() + "%");

                    // Add first page results and add it to its own group
                    datasetsGroups.Find(d => d.DatabaseCode == database.DatabaseCode).Datasets.AddRange(response.Datasets);

                    return response;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error happened in page:" + page);
                    return new DatasetsResponse();
                }
            }
        }

        private static async Task DownloadDatasetsAsync(int fromPage, int toPage, QuandlDatabase database)
        {
            // Fix console cursor position
            //if (firstTime) { HotFixConsoleCursor(); }

            var pages = Enumerable.Range(fromPage, toPage - 1);
            await Task.WhenAll(pages.Select(i => DownloadDatasetsAsync(i, database)));
        }

        private static async Task DownloadDatasetsAsync(int page, QuandlDatabase database)
        {
            using (WebClient client = new WebClient())
            {
                string data = await client.DownloadStringTaskAsync(new Uri("https://www.quandl.com/api/v3/datasets.json?database_code=" + database.DatabaseCode + "&sort_by=id&page=" + page + "&api_key=" + Utils.Constants.API_KEY));
                DatasetsResponse response =
                        JsonConvert.DeserializeObject<DatasetsResponse>(data, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                pagesSum++;
                Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets [" + database.DatabaseCode + "]: ", Utils.Helpers.GetPercent(pagesSum, response.Meta.TotalPages).ToString() + "%");

                // Add it to its own group
                datasetsGroups.Find(d => d.DatabaseCode == database.DatabaseCode).Datasets.AddRange(response.Datasets);
                //datasets.AddRange(response.Datasets);
            }
        }
    }
}
