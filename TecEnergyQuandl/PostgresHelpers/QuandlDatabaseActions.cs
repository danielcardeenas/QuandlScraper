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
    public static class QuandlDatabaseActions
    {
        public static void InsertQuandlDatabases(List<QuandlDatabase> databases)
        {
            using (var conn = new NpgsqlConnection(Utils.Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = QuandlDatabasesInsertQuery(databases);

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        conn.Close();
                        Helpers.ExitWithError(ex.Message);
                    }

                    ConsoleInformer.PrintProgress("2A", "Inserting new quandl databases: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        public static List<QuandlDatabase> GetImportedDatabases()
        {
            // Query
            string query = @"SELECT id, name, databasecode, description, datasetscount, downloads, premium, image, favorite, import
                                    FROM public.databases
                                    WHERE import = true";

            List<QuandlDatabase> databases = new List<QuandlDatabase>();

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
                            databases.Add(QuandlDatabase.MakeQuandlDatabase(dr));
                    }
                    catch (Exception ex)
                    {
                        conn.Close();
                        Helpers.ExitWithError(ex.Message);
                    }

                    ConsoleInformer.PrintProgress("0B", "Querying imported databases: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }

            return databases;
        }

        private static string QuandlDatabasesInsertQuery(List<QuandlDatabase> databases)
        {
            // Reference last item
            var last = databases.Last();

            string query = "INSERT INTO public.databases(id, name, databasecode, description, datasetscount, downloads, premium, image, favorite) VALUES ";
            foreach(QuandlDatabase item in databases)
            {
                query += String.Format(@"({0}, '{1}', '{2}', '{3}', {4}, {5}, {6}, '{7}', {8})",
                                    item.Id, item.Name, item.DatabaseCode, item.Description, item.DatasetsCount, item.Downloads, item.Premium, item.Image, item.Favorite);

                if (item != last)
                    query += ",\n";
            }

            query += "\nON CONFLICT(id) DO NOTHING";
            return query;
        }
    }
}
