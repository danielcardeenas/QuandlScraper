using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;

namespace TecEnergyQuandl
{
    public static class FetchData
    {
        private static List<QuandlDatasetGroup> datasetsGroups;
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



            // Make datasets model tables
            PostgresHelpers.QuandlDatasetActions.InsertQuandlDatasetsData(datasetsGroups);
        }
    }
}
