using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl.PostgresHelpers
{
    public static class SchemaActions
    {
        public static void CreateSchema()
        {
            InitPostgresDB();
            CreateQuandlDatabasesTable();
        }

        private static void InitPostgresDB()
        {
            Console.WriteLine("Creating inital schema\n---------------------------------------");
            using (var conn = new NpgsqlConnection(Constants.BASE_CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = @"CREATE DATABASE quandl
                            WITH 
                            OWNER = postgres
                            ENCODING = 'UTF8'
                            CONNECTION LIMIT = -1;

                            COMMENT ON DATABASE quandl
                            IS '
                            ';";

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        if (ex.SqlState == "42P04")
                        {
                            ConsoleInformer.Inform("Database already exist. Using it");
                        }
                        else { conn.Close(); Helpers.ExitWithError(ex.Message); }
                    }

                    ConsoleInformer.PrintProgress("0A", "Creating schema: ", "50%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        private static void CreateQuandlDatabasesTable()
        {
            using (var conn = new NpgsqlConnection(Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = @"CREATE TABLE Databases(
                                       Id               BIGINT  PRIMARY KEY NOT NULL,
                                       Name             TEXT    NOT NULL,
                                       DatabaseCode     TEXT,
                                       Description      TEXT,
                                       DatasetsCount    BIGINT,
                                       Downloads        BIGINT,
                                       Premium          BOOL    DEFAULT FALSE,
                                       Image            TEXT,
                                       Favorite         BOOL    DEFAULT FALSE,
                                       Import           BOOL    DEFAULT FALSE
                                    );";

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        //Console.WriteLine(ex.Message);
                        if (ex.SqlState == "42P07")
                        {
                            ConsoleInformer.Inform("QuandlDatabases table already exists. Using it");
                            //cmd.CommandText = "TRUNCATE TABLE databases";
                            //try { cmd.ExecuteNonQuery(); }
                            //catch (PostgresException exception)
                            //{
                            //    conn.Close(); Helpers.ExitWithError(exception.Message);
                            //}
                        }
                        else { conn.Close(); Helpers.ExitWithError(ex.Message); }
                    }

                    ConsoleInformer.PrintProgress("0A", "Creating schema: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }
    }
}
