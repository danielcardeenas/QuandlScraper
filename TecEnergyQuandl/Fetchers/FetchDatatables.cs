using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;
using TecEnergyQuandl.Model.ResponseHelpers;
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl.Fetchers
{
    public class FetchDatatables
    {
        private static List<QuandlDatatable> datatables;
        private static List<Tuple<string, string>> errors = new List<Tuple<string, string>>();

        private static int count = 0;
        public static void BeginDownloadData()
        {
            // Download first page and check meta
            Console.WriteLine("Fetching datatables available\n---------------------------------------");
            datatables = PostgresHelpers.QuandlDatatableActions.GetImportedDatatables();

            Console.WriteLine("\nSelected datatables:");
            datatables.ForEach(dt =>
                Console.WriteLine(" -[DT] " + dt.Name)
            );
            Console.WriteLine();

            foreach (QuandlDatatable datatable in datatables)
            {
                // Starting 0%
                count++;

                // Get first datasets page ordered
                var datatableResponse = DownloadDatatable(datatable);

                // Complete dataset
                datatable.Data = new List<object>();
                datatable.Columns = new List<QuandlColumn>();

                datatable.Data.AddRange(datatableResponse.Datatable.Data);
                datatable.Columns.AddRange(datatableResponse.Datatable.Columns);
            }

            Utils.ConsoleInformer.InformSimple("-------------------------------------");

            // Check errors
            if (errors.Count > 0)
            {
                Utils.ConsoleInformer.Inform("Some unexpected stuff happened. See the log for more info");

                foreach (var error in errors)
                {
                    // Write
                    using (StreamWriter sw = File.AppendText("log.txt"))
                    {
                        Utils.Helpers.Log(error.Item1, error.Item2, sw);
                    }
                }
            }

            // Make datatables model tables and insert data
            PostgresHelpers.QuandlDatatableActions.MakeQuandlDatatables(datatables);
        }


        private static DatatableResponse DownloadDatatable(QuandlDatatable datatable)
        {
            using (PacientWebClient client = new PacientWebClient())
            {
                try
                {
                    var json = client.DownloadString(" https://www.quandl.com/api/v3/datatables/" + datatable.Name + ".json?api_key=" + Utils.Constants.API_KEY);
                    DatatableResponse response =
                        JsonConvert.DeserializeObject<DatatableResponse>(json, new JsonSerializerSettings { ContractResolver = Utils.Converters.MakeUnderscoreContract() });

                    Utils.ConsoleInformer.PrintProgress("1B", "Fetching datatable [" + datatable.Name + "]: ", Utils.Helpers.GetPercent(count, datatables.Count).ToString() + "%");

                    return response;

                }
                catch (Exception e)
                {
                    // Add error to inform and log later
                    errors.Add(new Tuple<string, string>("Failed to fetch datatable: [" + datatable.Name + "]", "Ex: " + e.Message));
                    return new DatatableResponse();
                }
            }
        }
    }
}
