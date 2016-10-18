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
using TecEnergyQuandl.Model.Quandl;
using Newtonsoft.Json.Serialization;
using Npgsql;


namespace TecEnergyQuandl
{
    public class FetchDatabases
    {
        // Init databases list
        public static List<QuandlDatabase> databases = new List<QuandlDatabase>();

        public static async Task BeginDownloadDatabases()
        {
            using (WebClient client = new WebClient())
            {
                // Make first call to figure out how many pages are there
                // 100 databases or less per page

                // Download first page and check meta
                Console.WriteLine("Fetching databases\n---------------------------------------");
                var databaseReponse = DownloadDatabase(1);

                // To let user knows
                Utils.ConsoleInformer.PrintProgress("1A", "Fetching databases: ", Utils.Helpers.GetPercent(1, databaseReponse.Meta.TotalPages).ToString() + "%");

                // Save first database
                databases.AddRange(databaseReponse.Databases);

                // Download remaining databases
                await DownloadDatabasesAsync(2, databaseReponse.Meta.TotalPages);

                // Manipulate data into database
                Console.WriteLine("\nInserting into database\n---------------------------------------");
                PostgresHelpers.QuandlDatabaseActions.InsertQuandlDatabases(databases);

                return;
            }
        }

        private static DatabasesResponse DownloadDatabase(int page)
        {
            using (WebClient client = new WebClient())
            {
                var json = client.DownloadString(Utils.Constants.DATABASES_URL + "?page=" + page + "&api_key=" + Utils.Constants.API_KEY);
                DatabasesResponse response =
                    JsonConvert.DeserializeObject<DatabasesResponse>(json, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                return response;
            }
        }

        private static async Task DownloadDatabasesAsync(int fromPage, int toPage)
        {
            var pages = Enumerable.Range(fromPage, toPage - 1);
            await Task.WhenAll(pages.Select(i => DownloadDatabasesAsync(i)));
        }

        private static async Task DownloadDatabasesAsync(int page)
        {
            using (WebClient client = new WebClient())
            {
                string data = await client.DownloadStringTaskAsync(new Uri(Utils.Constants.DATABASES_URL + "?page=" + page + "&api_key=" + Utils.Constants.API_KEY));
                DatabasesResponse response =
                        JsonConvert.DeserializeObject<DatabasesResponse>(data, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                Utils.ConsoleInformer.PrintProgress("1A", "Fetching databases: ", Utils.Helpers.GetPercent(page, response.Meta.TotalPages).ToString() + "%");
                databases.AddRange(response.Databases);
            }
        }
    }
}
