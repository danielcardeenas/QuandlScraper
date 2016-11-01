using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl.PostgresHelpers
{
    public class QuandlDatasetActions
    {
        /**
         * Dataset info methos
         */
        public static void InsertQuandlDatasets(List<QuandlDatasetGroup> datasetsGroups)
        {
            // Make datasets model tables
            SchemaActions.CreateQuandlDatasetTable();

            // Insert datasets
            int count = 0;
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
            {
                count++;
                try
                {
                    throw new Exception();
                    datasetGroup.MakeInsertQuery();
                    ConsoleInformer.PrintProgress("3B", "Inserting [" + datasetGroup.DatabaseCode + "] datasets: ", Utils.Helpers.GetPercent(count, datasetsGroups.Count).ToString() + "%");
                }
                catch (Exception ex)
                {
                    // Write
                    Utils.ConsoleInformer.Inform("Some unexpected stuff happened. See the log for more info");

                    using (StreamWriter sw = File.AppendText("log.txt"))
                    {
                        Utils.Helpers.Log("Something worng happened when trying to insert [" + datasetGroup.DatabaseCode + "] datasets. Check log", 
                                        ex.Message, sw);
                    }
                }
            }
        }

        [Obsolete("Use MakeInsertQuery() instead")]
        public static void InsertQuandlDatasets(QuandlDatasetGroup datasetGroup)
        {
            // Make query before oppening the connection to make sure it doesn't time out.
            string queryFilePath = datasetGroup.MakeInsertQueryFile();

            FileInfo file = new FileInfo(queryFilePath);
            string query = file.OpenText().ReadToEnd();

            using (var conn = new NpgsqlConnection(Utils.Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

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

        /**
         * Dataset data methos
         */
        public static List<QuandlDatasetGroup> GetImportedDatasets()
        {
            // This query does not takes in count if the de dataset's database is imported too
            //string query = @"SELECT " + QuandlDataset.GetColumnsForQuery() + " " +
            //                        @"FROM quandl.datasets
            //                        WHERE import = true";

            // Query
            string query = @"SELECT " + QuandlDataset.GetColumnsForQuerySuffixed("ds") + @" 
                            FROM quandl.databases INNER JOIN quandl.datasets ds ON (quandl.databases.databasecode = ds.databasecode)
                            WHERE quandl.databases.import = true AND ds.import = true";

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

        // Detects the newest data date
        // Returns a list of pairs:
        //  [date, datasetcode]
        //  Ex. ['2016-10-21', 'AAPL']
        //      ['2016-10-26', 'FB']
        //      ...
        public static List<Tuple<DateTime, string>> GetNewestImportedData(QuandlDatasetGroup datasetGroup)
        {
            // Query
            string query = @"SELECT max(date) as date, datasetcode 
                            FROM quandl." + datasetGroup.DatabaseCode + @" 
                            GROUP BY datasetcode";

            List<Tuple<DateTime, string>> datasetNewestDateList = new List<Tuple<DateTime, string>>();
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

                        // Make tuple for each row
                        // Add tuple to list
                        while (dr.Read())
                            datasetNewestDateList.Add(new Tuple<DateTime, string>(dr.GetDateTime(dr.GetOrdinal("date")), (string)dr["datasetcode"]));
                    }
                    catch (PostgresException ex)
                    {
                        if (ex.SqlState == "42P01")
                        {
                            // The table does not exists.
                            // That means this is the first time importing this dataset group
                            datasetNewestDateList.Add(new Tuple<DateTime, string>(DateTime.MinValue, datasetGroup.Datasets[0].DatasetCode));
                        }
                        else
                        {
                            conn.Close();
                            Helpers.ExitWithError(ex.Message);
                        }
                    }

                    //ConsoleInformer.PrintProgress("0C", "Querying imported datasets: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }

            return datasetNewestDateList;
        }

        public static void InsertQuandlDatasetsData(List<QuandlDatasetGroup> datasetsGroups)
        {
            // Make datasets model tables
            Console.WriteLine("Creating unique table model for datasets:");
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
                SchemaActions.CreateQuandlDatasetDataTable(datasetGroup);

            // Insert data
            int count = 0;
            foreach (QuandlDatasetGroup datasetGroup in datasetsGroups)
            {
                count++;
                Console.WriteLine("\nCreating query for datasets in group: [" + datasetGroup.DatabaseCode + "] (" + count + "/" + datasetsGroups.Count + ")");
                datasetGroup.MakeInsertDataQuery();
                Utils.ConsoleInformer.PrintProgress("3C", "Inserting data for group[" + datasetGroup.DatabaseCode + "]: ", "100%");
            }
        }

        [Obsolete("Use MakeInsertDataQuery() instead")]
        private static void InsertQuandlDatasetData(QuandlDatasetGroup datasetGroup)
        {
            string query = "";
            using (var conn = new NpgsqlConnection(Utils.Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    datasetGroup.MakeInsertDataQuery();

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
