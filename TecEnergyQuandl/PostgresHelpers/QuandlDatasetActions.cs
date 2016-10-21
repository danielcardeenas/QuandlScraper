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
            int count = 0;
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
            {
                count++;
                InsertQuandlDatasets(datasetGroup);
                ConsoleInformer.PrintProgress("3B", "Inserting [" + datasetGroup.DatabaseCode + "] datasets: ", Utils.Helpers.GetPercent(count, datasetsGroups.Count).ToString() + "%");
            }
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

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        public static List<QuandlDatasetGroup> GetImportedDatasets()
        {
            // Query
            string query = @"SELECT " + QuandlDataset.GetColumnsForQuery() + " " +
                                    @"FROM quandl.datasets
                                    WHERE import = true";

            List<QuandlDatasetGroup> datasetsGroups = new List<QuandlDatasetGroup>();
            using (var conn = new NpgsqlConnection(Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand(query))
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();
                    cmd.Connection = conn;

                    try
                    {
                        // Execute the query and obtain a result set
                        NpgsqlDataReader dr = cmd.ExecuteReader();

                        // Each row
                        while (dr.Read())
                        {
                            // Add each dataset to its own group (Database code)
                            QuandlDataset dataset = QuandlDataset.MakeQuandlDataset(dr);

                            // If group doesnt exists, create it
                            if (!datasetsGroups.Exists(d => d.DatabaseCode == dataset.DatabaseCode))
                                datasetsGroups.Add(new QuandlDatasetGroup() { DatabaseCode = dataset.DatabaseCode, Datasets = new List<QuandlDataset>() });

                            datasetsGroups.Find(d => d.DatabaseCode == dataset.DatabaseCode).Datasets.Add(dataset);
                        }
                    }
                    catch (Exception ex)
                    {
                        conn.Close();
                        Helpers.ExitWithError(ex.Message);
                    }

                    ConsoleInformer.PrintProgress("0C", "Querying imported datasets: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }

            return datasetsGroups;
        }

        public static void InsertQuandlDatasetsData(List<QuandlDatasetGroup> datasetsGroups)
        {
            // Make datasets model tables
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
                SchemaActions.CreateQuandlDatasetDataTable(datasetGroup);

            // Insert data
            int count = 0;
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
            {
                count++;
                //ConsoleInformer.PrintProgress("3C", "Inserting data [" + datasetGroup.DatabaseCode + "/" + datasetGroup.da + "]: ", Helpers.GetPercent(count, datasetsGroups.Count).ToString() + "%");
                InsertQuandlDatasetData(datasetGroup);
            }
        }

        private static void InsertQuandlDatasetData(QuandlDatasetGroup datasetGroup)
        {
            using (var conn = new NpgsqlConnection(Utils.Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = datasetGroup.MakeInsertDataQuery();

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        conn.Close();
                        Helpers.ExitWithError(ex.Message);
                    }

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }
    }
}
