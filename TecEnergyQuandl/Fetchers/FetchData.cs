using Newtonsoft.Json;
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
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl
{
    public static class FetchData
    {
        private static List<QuandlDatasetGroup> datasetsGroups;
        private static List<Tuple<string, string>> errors = new List<Tuple<string, string>>();

        private static int datasetsFetched = 0;
        private static bool blocked = false;
        public static async Task BeginDownloadData()
        {
            // Download first page and check meta
            Console.WriteLine("Fetching datasets\n---------------------------------------");
            datasetsGroups = PostgresHelpers.QuandlDatasetActions.GetImportedDatasets();

            Console.WriteLine("\nSelected datasets models - quantity:");
            datasetsGroups.ForEach(d =>
                Console.WriteLine(" -[DB Model] " + d.DatabaseCode + " - " + d.Datasets.Count)
            );
            Console.WriteLine();

            Console.WriteLine("\nDetecting newest data available:");
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
            {
                List<Tuple<DateTime, string>> datasetNewestDateList = PostgresHelpers.QuandlDatasetActions.GetNewestImportedData(datasetGroup);

                // Item1 = Newest date of data
                // Item2 = Dataset code
                foreach (var tuple in datasetNewestDateList)
                {
                    // Will only add those who dataset is imported
                    QuandlDataset dataset = datasetGroup.Datasets.Find(d => d.DatasetCode == tuple.Item2);
                    if (dataset != null) { dataset.LastFetch = tuple.Item1; }
                }
            }

            int count = 0;
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
            {
                // Update groups to fetched count
                count++;

                // Identify current group
                Utils.ConsoleInformer.InformSimple("Group model: [" + datasetGroup.DatabaseCode + "]. Group:" + count + "/" + datasetsGroups.Count);

                // Request all datasets from group
                await DownloadDatasetsDataAsync(datasetGroup, datasetGroup.Datasets.Count);
            }

            // Check errors
            if (errors.Count > 0)
            {
                Utils.ConsoleInformer.Inform("Some unexpected stuff happened. See the log for more info");
            }

            // Make datasets model tables
            PostgresHelpers.QuandlDatasetActions.InsertQuandlDatasetsData(datasetsGroups);
        }

        private static async Task DownloadDatasetsDataAsync(QuandlDatasetGroup datasetGroup, int to)
        {
            await Task.WhenAll(datasetGroup.Datasets.Select(d => DownloadDatasetDataAsync(d, to)));
            Console.WriteLine();

            // Reset datasets fetched count for next group
            datasetsFetched = 0;
        }

        private static async Task DownloadDatasetDataAsync(QuandlDataset dataset, int to)
        {
            using (PacientWebClient client = new PacientWebClient())
            {
                try
                {
                    string data = await client.DownloadStringTaskAsync(new Uri("https://www.quandl.com/api/v3/datasets/" + dataset.DatabaseCode +
                                                                            "/" + dataset.DatasetCode + "/data.json?api_key=" + Utils.Constants.API_KEY +
                                                                            "&start_date=" + dataset.LastFetch.GetValueOrDefault(DateTime.MinValue).AddDays(1).ToString("yyyy-MM-dd"))); // Add one day because I dont want to include the current newest in the json
                    DataResponse response =
                            JsonConvert.DeserializeObject<DataResponse>(data, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                    QuandlDatasetData datasetData = response.DatasetData;
                    datasetData.SetBaseDataset(dataset);

                    datasetsFetched++;
                    using (var mutex = new Mutex(false, "SHARED_FETCH_DATA"))
                    {
                        mutex.WaitOne();
                        // Start process
                        // ===============================================
                        Utils.ConsoleInformer.PrintProgress("1C", "Fetching dataset [" + dataset.DatasetCode + "]: ", Utils.Helpers.GetPercent(datasetsFetched, to).ToString() + "%");

                        // End process
                        // ===============================================
                        mutex.ReleaseMutex();
                    }

                    // Replace old uncomplete dataset with new one
                    ReplaceCompleteDataset(datasetData);
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("(429)") && !blocked)
                    {
                        Utils.ConsoleInformer.Inform("Looks like quandl just blocked you");
                        blocked = true;
                    }

                    // Log
                    using (var mutex = new Mutex(false, "SHARED_LOG_DATA"))
                    {
                        mutex.WaitOne();
                        // Start process
                        // ===============================================
                        using (StreamWriter sw = File.AppendText("log.txt"))
                        {
                            Utils.Helpers.Log("Failed to fetch data: from dataset: [" + dataset.DatabaseCode + "/" + dataset.DatasetCode + "]", "Ex: " + e.Message, sw);
                        }

                        // End process
                        // ===============================================
                        mutex.ReleaseMutex();
                    }

                    errors.Add(new Tuple<string, string>("Failed to fetch data: from dataset: [" + dataset.DatabaseCode + "/" + dataset.DatasetCode + "]", "Ex: " + e.Message));
                }
            }
        }

        private static void ReplaceCompleteDataset(QuandlDatasetData datasetData)
        {
            // Reference current groups list
            var currentDatasetGroup = datasetsGroups
                    .Find(dg => dg.DatabaseCode == datasetData.DatabaseCode)
                    .Datasets;
            
            // Index of the dataset to update
            var index = currentDatasetGroup.IndexOf(currentDatasetGroup.First(ds => ds.Id == datasetData.Id));

            // Replace
            if (index != -1)
                currentDatasetGroup[index] = datasetData;
        }
    }
}
