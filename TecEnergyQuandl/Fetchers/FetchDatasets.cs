using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;
using TecEnergyQuandl.Model.ResponseHelpers;
using TecEnergyQuandl.PostgresHelpers;
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl
{
    public static class FetchDatasets
    {
        private static List<QuandlDatabase> databases;
        private static List<QuandlDatasetGroup> datasetsGroups = new List<QuandlDatasetGroup>();
        private static List<Tuple<string, string>> errors = new List<Tuple<string, string>>();

        private static int pagesSum;
        private static bool blocked = false;
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

            // Prepare schema:
            // Make datasets model tables
            SchemaActions.CreateQuandlDatasetTable();
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
                    //DownloadDatasetsParallel(2, datasetsReponse.Meta.TotalPages, database);

                Utils.ConsoleInformer.InformSimple("[DB] " + database.DatabaseCode + " Done. [" + count + "/" + databases.Count + "]");
                Utils.ConsoleInformer.InformSimple("-------------------------------------");
            }

            // Check errors
            if (errors.Count > 0)
            {
                Utils.ConsoleInformer.Inform("Some unexpected stuff happened. See the log for more info");
            }

            // Manipulate data into database
            //Console.WriteLine("\nInserting data into database\n---------------------------------------");

            // Make datasets list
           //PostgresHelpers.QuandlDatasetActions.InsertQuandlDatasets(datasetsGroups);
        }

        private static DatasetsResponse DownloadDataset(int page, QuandlDatabase database)
        {
            using (PatientWebClient client = new PatientWebClient())
            {
                try
                {
                    var json =  client.DownloadString("https://www.quandl.com/api/v3/datasets.json?database_code=" + database.DatabaseCode + "&sort_by=id&page=" + page + "&api_key=" + Utils.Constants.API_KEY);
                    DatasetsResponse response =
                        JsonConvert.DeserializeObject<DatasetsResponse>(json, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                    pagesSum++;
                    Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets [" + database.DatabaseCode + "]: ", Utils.Helpers.GetPercent(pagesSum, response.Meta.TotalPages).ToString() + "%");

                    // Add it to its own group
                    datasetsGroups.Find(d => d.DatabaseCode == database.DatabaseCode).Datasets.AddRange(response.Datasets);

                    // Insert datasets page directly
                    QuandlDatasetGroup datasetGroup = new QuandlDatasetGroup() { DatabaseCode = database.DatabaseCode, Datasets = response.Datasets};
                    PostgresHelpers.QuandlDatasetActions.InsertQuandlDatasetGroup(datasetGroup);

                    return response;

                }
                catch (Exception e)
                {
                    if (e.Message.Contains("(429)") && !blocked)
                    {
                        Utils.ConsoleInformer.Inform("Looks like quandl just blocked you");
                        blocked = true;
                    }

                    // Log
                    Utils.Helpers.Log("Failed to fetch page: " + page + " from Database: [" + database.DatabaseCode + "]", "Ex: " + e.Message);

                    // Add error to inform and log later
                    errors.Add(new Tuple<string, string> ("Failed to fetch page: " + page + " from Database: [" + database.DatabaseCode + "]", "Ex: " + e.Message));
                    return new DatasetsResponse();
                }
            }
        }

        private static async Task DownloadDatasetsAsync(int fromPage, int toPage, QuandlDatabase database)
        {
            var pages = Enumerable.Range(fromPage, toPage - 1);
            await Task.WhenAll(pages.Select(i => DownloadDatasetsAsync(i, database)));
        }

        private static async Task DownloadDatasetsAsync(int page, QuandlDatabase database)
        {
            using (PatientWebClient client = new PatientWebClient())
            {
                try
                {
                    string json = await client.DownloadStringTaskAsync(new Uri("https://www.quandl.com/api/v3/datasets.json?database_code=" + database.DatabaseCode + "&sort_by=id&page=" + page + "&api_key=" + Utils.Constants.API_KEY));
                    DatasetsResponse response =
                        JsonConvert.DeserializeObject<DatasetsResponse>(json, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                    pagesSum++;
                    Utils.ConsoleInformer.PrintProgress("1B", "Fetching datasets [" + database.DatabaseCode + "]: ", Utils.Helpers.GetPercent(pagesSum, response.Meta.TotalPages).ToString() + "%");

                    // Add it to its own group
                    //datasetsGroups.Find(d => d.DatabaseCode == database.DatabaseCode).Datasets.AddRange(response.Datasets);

                    // Insert datasets page directly
                    QuandlDatasetGroup datasetGroup = new QuandlDatasetGroup() { DatabaseCode = database.DatabaseCode, Datasets = response.Datasets };
                    PostgresHelpers.QuandlDatasetActions.InsertQuandlDatasetGroup(datasetGroup);
                }
                catch (Exception e)
                {
                    // Blocked?
                    if (e.Message.Contains("(429)") && !blocked)
                    {
                        Utils.ConsoleInformer.Inform("Looks like quandl just blocked you");
                        blocked = true;
                    }

                    // Log
                    Utils.Helpers.Log("Failed to fetch page: " + page + " from Database: [" + database.DatabaseCode + "]", "Ex: " + e.Message);

                    // Add error to inform and log later
                    errors.Add(new Tuple<string, string>("Failed to fetch page: " + page + " from Database: [" + database.DatabaseCode + "]", "Ex: " + e.Message));
                }
            }
        }
    }
}
