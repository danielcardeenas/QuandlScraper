using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl.PostgresHelpers
{
    public class QuandlDatasetActions
    {
        public static void InsertQuandlDatasets(List<QuandlDatasetGroup> datasetsGroups)
        {
            // Make datasets model tables
            SchemaActions.CreateQuandlDatasetTable();

            // Insert datasets
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
                InsertQuandlDatasets(datasetGroup);
        }

        public static void InsertQuandlDatasetsData(List<QuandlDatasetGroup> datasetsGroups)
        {
            // Make datasets model tables
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
                SchemaActions.CreateQuandlDatasetDataTable(datasetGroup);

            // Insert data
            //foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
            //    InsertQuandlDataset(datasetGroup);
        }

        public static void InsertQuandlDatasets(QuandlDatasetGroup datasetGroup)
        {
            using (var conn = new NpgsqlConnection(Utils.Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = datasetGroup.MakeInsertQuery();

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        conn.Close();
                        Helpers.ExitWithError(ex.Message);
                    }

                    ConsoleInformer.PrintProgress("2A", "Inserting new quandl datasets: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }
    }
}
