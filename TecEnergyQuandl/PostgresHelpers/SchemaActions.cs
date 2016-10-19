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
    public static class SchemaActions
    {
        public static void MakeDatabase()
        {
            Console.WriteLine("Creating inital schema\n---------------------------------------");
            InitPostgresDB();
            ConsoleInformer.PrintProgress("0A", "Creating schema: ", "50%");
            CreateQuandlSchema();
            ConsoleInformer.PrintProgress("0A", "Creating schema: ", "75%");
            CreateQuandlDatabasesTable();
            ConsoleInformer.PrintProgress("0A", "Creating schema: ", "100%");
        }

        private static void InitPostgresDB()
        {
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

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        private static void CreateQuandlSchema()
        {
            using (var conn = new NpgsqlConnection(Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = @"CREATE SCHEMA quandl;";

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        if (ex.SqlState == "42P06")
                        {
                            ConsoleInformer.Inform("Quandl shcema already exists. Using it");
                        }
                        else { conn.Close(); Helpers.ExitWithError(ex.Message); }
                    }

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
                    string query = @"CREATE TABLE quandl.Databases(
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

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        // Where all the datasets will go
        public static void CreateQuandlDatasetTable()
        {
            using (var conn = new NpgsqlConnection(Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = @"CREATE TABLE quandl.datasets (
                                        Id                   BIGINT  PRIMARY KEY NOT NULL,
                                        DatasetCode          TEXT,
                                        DatabaseCode         TEXT,
                                        Name                 TEXT    NOT NULL,
                                        Description          TEXT,
                                        NewestAvailableDate  DATE,
                                        OldestAvailableDate  DATE,
                                        ColumnNames          TEXT,
                                        Frequency            TEXT,
                                        Type                 TEXT,
                                        Premium              BOOL    DEFAULT FALSE,
                                        DatabaseId           BIGINT,
                                        Import               BOOL    DEFAULT FALSE
                                    );";

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        //Console.WriteLine(ex.Message);
                        if (ex.SqlState == "42P07")
                        {
                            ConsoleInformer.Inform("Datasets table model already exists. Using it");
                        }
                        else { conn.Close(); Helpers.ExitWithError(ex.Message); }
                    }

                    ConsoleInformer.PrintProgress("2B", "Creating datasets table: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        // Where all the datasets data will go
        public static void CreateQuandlDatasetDataTable(QuandlDatasetGroup datasetGroup)
        {
            using (var conn = new NpgsqlConnection(Constants.CONNECTION_STRING))
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // Open connection
                    // ===============================================================
                    conn.Open();

                    // Query
                    string query = @"CREATE TABLE quandl." + datasetGroup.DatabaseCode + @"(
                                        Id                   BIGINT  PRIMARY KEY NOT NULL,
                                        DatasetCode          TEXT,
                                        DatabaseCode         TEXT,
                                        Name                 TEXT    NOT NULL,
                                        Description          TEXT,
                                        NewestAvailableDate  DATE,
                                        OldestAvailableDate  DATE,
                                        Frequency            TEXT,
                                        Type                 TEXT,
                                        Premium              BOOL    DEFAULT FALSE,
                                        DatabaseId           BIGINT," +
                                        // Column names [specific data]
                                        datasetGroup.MakeDatasetsExtraColumns() + @"
                                    );";

                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    try { cmd.ExecuteNonQuery(); }
                    catch (PostgresException ex)
                    {
                        //Console.WriteLine(ex.Message);
                        if (ex.SqlState == "42P07")
                        {
                            ConsoleInformer.Inform("QuandlDataset table model already exists. Using it");
                        }
                        else { conn.Close(); Helpers.ExitWithError(ex.Message); }
                    }

                    ConsoleInformer.PrintProgress("2B", "[" + datasetGroup.DatabaseCode + "] Creating table model: ", "100%");

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }
    }
}
