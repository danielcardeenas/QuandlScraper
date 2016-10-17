using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model;
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

                    ConsoleInformer.PrintProgress("2A", "Inserting quandl databases: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
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
