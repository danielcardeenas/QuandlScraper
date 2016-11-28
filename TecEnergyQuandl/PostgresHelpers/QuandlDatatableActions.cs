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
    public class QuandlDatatableActions
    {
        public static void InsertQuandlDatatables()
        {
            using (var conn = new NpgsqlConnection(Utils.Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = QuandlDatatablesInsertQuery();

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        conn.Close();
                        Helpers.ExitWithError(ex.Message);
                    }

                    ConsoleInformer.PrintProgress("1D", "Inserting quandl datatables: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        private static string QuandlDatatablesInsertQuery()
        {
            // Reference last item
            var last = Utils.Constants.DATATABLES.Last();

            string query = @"WITH data(name, import, date_insert) as ( values";
            foreach (string item in Utils.Constants.DATATABLES)
            {
                query += String.Format(@"('{0}', {1}, date_trunc('second',current_timestamp))",
                                    item, "false");

                if (item != last)
                    query += ",\n";
                else
                    query += ")";
            }

            query += "\nINSERT INTO quandl.datatables (name, import, date_insert)" +
                    " SELECT distinct on (name) name, import, date_insert" +
                    " FROM data" +
                    " WHERE NOT EXISTS (SELECT 1 FROM quandl.datatables dt WHERE dt.name = data.name)";
            return query;
        }

        public static List<QuandlDatatable> GetImportedDatatables()
        {
            // Query
            string query = @"SELECT name
                                    FROM quandl.datatables
                                    WHERE import = true";

            List<QuandlDatatable> databases = new List<QuandlDatatable>();
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
                            databases.Add(QuandlDatatable.MakeQuandlDatabase(dr));
                    }
                    catch (Exception ex)
                    {
                        conn.Close();
                        Helpers.ExitWithError(ex.Message);
                    }

                    ConsoleInformer.PrintProgress("2D", "Querying imported databases: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }

            return databases;
        }

        public static void MakeQuandlDatatables(List<QuandlDatatable> datatables)
        {
            // Make datatable model tables
            Console.WriteLine("Creating unique table model for datatables:");
            foreach (QuandlDatatable datatable in datatables)
                SchemaActions.CreateQuandlDatatableModelTable(datatable);

            // Insert data
            int count = 0;
            foreach (QuandlDatatable datatable in datatables)
            {
                count++;
                Console.WriteLine("\nCreating query for datatable: [" + datatable.Name + "] (" + count + "/" + datatables.Count + ")");
                datatable.MakeInsertQuery();
                Utils.ConsoleInformer.PrintProgress("3C", "Inserting data for group[" + datatable.Name + "]: ", "100%");
            }
        }
    }
}
