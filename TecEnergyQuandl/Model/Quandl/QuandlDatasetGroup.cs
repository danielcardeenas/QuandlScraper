using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Remoting;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl.Model.Quandl
{
    public class QuandlDatasetGroup
    {
        public string DatabaseCode { get; set; }
        public List<QuandlDataset> Datasets { get; set; }

        private List<string> queries;
        private string queryFilePath;

        // Return extra columns
        // Ex. For WIKI:
        //  Date, Vol, High, Low, etc..
        //
        // Each Database has its own extra columns
        public List<string> ColumnNames()
        {
            return Datasets.ElementAt(0).ColumnNames;
        }

        // Creates query to insert dataset
        public string MakeInsertQueryFile()
        {
            // Init bulk query file
            InitFile();

            // Inital part
            string query = @"WITH data(" + QuandlDataset.GetColumnsForQuery() + @") as ( values";
            WriteToQueryFile(query);

            // Data elements to be formated for each thread
            int elementsPerThread = 1000;

            // Init where all queries generated will belong
            queries = new List<string>();

            // Init taks
            var tasks = new List<Task>();

            // Create query only if needed
            if (Datasets.Count > 0)
                tasks.AddRange(CreateQueryThreadsFile(elementsPerThread));
            else
                Utils.ConsoleInformer.Inform("Database [" + DatabaseCode + "] is already in its last version");

            // If nothing to do just skip
            if (tasks.Count <= 0)
                return "";

            // Wait for all the threads to complete
            Task.WaitAll(tasks.ToArray());

            // Remove last comma ","
            RemoveLastCommaInQueryFile();

            WriteToQueryFile(")"); // Close (values ... ) 

            WriteToQueryFile("\nINSERT INTO quandl.datasets (" + QuandlDataset.GetColumnsForQuery() + ")" +
                    " SELECT " + QuandlDataset.GetColumnsForQuery() +
                    " FROM data" +
                    " WHERE NOT EXISTS (SELECT 1 FROM quandl.datasets ds WHERE ds.Id = data.Id)");

            return queryFilePath;
        }

        // Creates query to insert dataset
        public void MakeInsertQuery()
        {
            // Data elements to be formated for each thread
            int elementsPerThread = 500;

            // Init where all queries generated will belong
            queries = new List<string>();

            // Init taks
            var tasks = new List<Task>();

            // Create query only if needed
            if (Datasets.Count > 0)
                tasks.AddRange(CreateQueryThreads(elementsPerThread));
            else
                Utils.ConsoleInformer.Inform("Database [" + DatabaseCode + "] is already in its last version");

            // If nothing to do just skip
            if (tasks.Count <= 0)
                return;

            // Wait for all the threads to complete
            Task.WaitAll(tasks.ToArray());
        }

        // Creates query to insert data
        public string MakeInsertDataQuery()
        {
            string query = @"WITH data(" + GetColumnsForInsertDataQuery() + @") as ( values";

            // Data elements to be formated for each thread
            int elementsPerThread = 500;

            // Init where all queries generated will belong
            queries = new List<string>();

            // Init taks
            var tasks = new List<Task>();
            foreach (QuandlDatasetData item in Datasets)
            {
                // Create query only if needed
                if (item.Data.Count > 0)
                    tasks.AddRange(CreateDataQueryThreads(item, elementsPerThread));
                else
                    Utils.ConsoleInformer.Inform("Dataset [" + item.DatasetCode + "] is already in its last version");
            }

            // If nothing to do just skip
            if (tasks.Count <= 0)
                return "";

            // Wait for all the threads to complete
            Task.WaitAll(tasks.ToArray());

            // Join all the querys
            string values = String.Join("\n", queries);

            // Remove last comma ","
            values = values.Remove(values.Length - 1);

            // Join "insert into... values" + "(...)"
            query += values;
            query += ")"; // Close (values ... ) 
            query += "\nINSERT INTO quandl." + DatabaseCode + "(" + GetColumnsForInsertDataQuery() + ")" +
                     " SELECT " + GetColumnsForInsertDataQuery() +
                     " FROM data";

            if (HasColumnDate())
            {
                query += @" WHERE NOT EXISTS (
                            SELECT 1 FROM quandl." + DatabaseCode + @" ds 
                                WHERE ds.date = data.date AND
                                      ds.datasetcode = data.datasetcode
                            )";
            }

            return query;
        }

        private bool HasColumnDate()
        {
            return ColumnNames().FirstOrDefault(x => x.ToLower() == "date") != null;
        }

        private int GetThreadsNeeded(int dataCount, int elementsPerThread)
        {
            return (int)Math.Ceiling(((double)dataCount / (double)elementsPerThread));
        }

        private Task[] CreateDataQueryThreads(QuandlDatasetData item, int elementsPerThread)
        {
            var tasks = new List<Task>();
            int threadsNeeded = GetThreadsNeeded(item.Data.Count, elementsPerThread);

            int remainingElements = item.Data.Count;
            int from = 0;
            for (int i = 0; i < threadsNeeded; i++)
            {
                int fromIndex = from;
                int toIndex = fromIndex + elementsPerThread;
                tasks.Add(Task.Factory.StartNew(() => CreatePartialDataQuery(item, fromIndex, toIndex)));

                from += elementsPerThread;
            }

            return tasks.ToArray();
        }

        private Task[] CreateQueryThreads(int elementsPerThread)
        {
            var tasks = new List<Task>();
            int threadsNeeded = GetThreadsNeeded(Datasets.Count, elementsPerThread);

            int remainingElements = Datasets.Count;
            int from = 0;
            for (int i = 0; i < threadsNeeded; i++)
            {
                int fromIndex = from;
                int toIndex = fromIndex + elementsPerThread;
                tasks.Add(Task.Factory.StartNew(() => CreatePartialQuery(fromIndex, toIndex)));

                from += elementsPerThread;
            }

            return tasks.ToArray();
        }

        private Task[] CreateQueryThreadsFile(int elementsPerThread)
        {
            var tasks = new List<Task>();
            int threadsNeeded = GetThreadsNeeded(Datasets.Count, elementsPerThread);

            int remainingElements = Datasets.Count;
            int from = 0;
            for (int i = 0; i < threadsNeeded; i++)
            {
                int fromIndex = from;
                int toIndex = fromIndex + elementsPerThread;
                tasks.Add(Task.Factory.StartNew(() => CreatePartialQueryFile(fromIndex, toIndex)));

                from += elementsPerThread;
            }

            return tasks.ToArray();
        }

        private void CreatePartialQueryFile(int from, int to)
        {
            if (to > Datasets.Count)
                to = Datasets.Count;

            string query = "";
            for (int i = from; i < to; i++)
            {
                // Current
                QuandlDataset item = Datasets[i];

                // Base insert
                query += String.Format(@"({0}, '{1}', '{2}', '{3}', '{4}', to_date('{5}', 'YYYY-MM_DD'), to_date('{6}', 'YYYY-MM_DD'), '{7}', '{8}', '{9}', {10}, {11}, {12})",
                                    item.Id, item.DatasetCode, item.DatabaseCode, item.Name, item.Description, // 0 - 4
                                    item.NewestAvailableDate.GetValueOrDefault(DateTime.Now).ToString("yyyy-MM-dd"), item.OldestAvailableDate.GetValueOrDefault(DateTime.Now).ToString("yyyy-MM-dd"), // 5 - 6
                                    string.Join(",", item.ColumnNames), // 7 
                                    item.Frequency, item.Type, // 8 - 9
                                    item.Premium, item.DatabaseId, item.Import); // 10 - 12
                query += ",";
            }

            // Write partial query to file
            WriteToQueryFile(query);
        }

        private void InitFile()
        {
            // Create folder & file
            Directory.CreateDirectory(DatabaseCode + "\\Datasets");

            // Create file
            string fileName = "ds" + DateTime.Now.ToString("yyyyMMdd") + ".txt";

            queryFilePath = Path.Combine(DatabaseCode + "\\Datasets", fileName);

            Console.WriteLine("Path to my file: {0}\n", queryFilePath);

            if (!File.Exists(queryFilePath))
                File.Create(queryFilePath);
        }

        private void WriteToQueryFile(string partial)
        {
            using (var mutex = new Mutex(false, "SHARED_INSERT_DATASETS"))
            {
                mutex.WaitOne();
                // Start process file
                // ===============================================
                File.AppendAllText(queryFilePath, partial);

                // End process file
                // ===============================================
                mutex.ReleaseMutex();
            }
        }

        private void RemoveLastCommaInQueryFile()
        {
            try
            {
                FileStream fs = new FileStream(queryFilePath, FileMode.Open, FileAccess.ReadWrite);
                fs.SetLength(fs.Length - 1);
                fs.Close();
            }
            catch (Exception ex) { }
        }

        private void CreatePartialQuery(int from, int to)
        {
            if (to > Datasets.Count)
                to = Datasets.Count;

            string query = @"WITH data(" + QuandlDataset.GetColumnsForQuery() + @") as ( values";
            //string query = "";
            for (int i = from; i < to; i++)
            {
                // Current
                QuandlDataset item = Datasets[i];

                // Base insert
                query += String.Format(@"({0}, '{1}', '{2}', '{3}', '{4}', to_date('{5}', 'YYYY-MM_DD'), to_date('{6}', 'YYYY-MM_DD'), '{7}', '{8}', '{9}', {10}, {11}, {12})",
                                    item.Id, item.DatasetCode, item.DatabaseCode, item.Name, item.Description, // 0 - 4
                                    item.NewestAvailableDate.GetValueOrDefault(DateTime.Now).ToString("yyyy-MM-dd"), item.OldestAvailableDate.GetValueOrDefault(DateTime.Now).ToString("yyyy-MM-dd"), // 5 - 6
                                    string.Join(",", item.ColumnNames), // 7 
                                    item.Frequency, item.Type, // 8 - 9
                                    item.Premium, item.DatabaseId, item.Import); // 10 - 12

                query += ",";
            }

            // Remove last comma ","
            query = query.Remove(query.Length - 1);
            query += ")"; // Close (values ... ) 

            query += "\nINSERT INTO quandl.datasets (" + QuandlDataset.GetColumnsForQuery() + ")" +
                    " SELECT " + QuandlDataset.GetColumnsForQuery() +
                    " FROM data" +
                    " WHERE NOT EXISTS (SELECT 1 FROM quandl.datasets ds WHERE ds.Id = data.Id)";

            //Execute query
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

        private void CreatePartialDataQuery(QuandlDatasetData dataset, int from, int to)
        {
            if (to > dataset.Data.Count)
                to = dataset.Data.Count;

            string query = "";
            for (int i = from; i < to; i++)
            {
                // Current
                object[] data = dataset.Data[i];

                // Base insert
                query += String.Format(@"('{0}', '{1}', '{2}', '{3}', {4}",
                                dataset.DatasetCode, dataset.DatabaseCode, dataset.Name, // 0 - 2
                                dataset.Transform, // 3
                                dataset.DatabaseId); // 4

                // Extra columns
                query += String.Format(FormatExtraColumns(0) + " ),",
                                data);
            }

            queries.Add(query);
        }

        private string FormatExtraColumns(int fromNumber)
        {
            string extraColumns = "";

            foreach(string column in ColumnNames())
            {
                extraColumns += ", " + PrepareExtraColumnFormated(column, fromNumber);
                fromNumber++;
            }

            return extraColumns;
        }

        private string PrepareExtraColumnFormated(string column, int number)
        {
            if (GetPostgresColumnType(column) == "TEXT")
                return "'{" + number + "}'";
            else if (GetPostgresColumnType(column) == "DATE")
                return "to_date('{" + number + "}', 'YYYY-MM_DD')";
            else
                return "{" + number + "}";
        }

        private string GetColumnsForInsertDataQuery()
        {
            string columns = @" DatasetCode,
                                DatabaseCode,
                                Name,
                                Transform,
                                DatabaseId," +
                                // Column names [specific data]
                                MakeDatasetsExtraColumns();

            return columns;
        }

        public string MakeDatasetsExtraColumns()
        {
            string columns = "";
            foreach (string column in ColumnNames())
            {
                columns += "\n" + column + ",";
            }

            // Return without the last comma ","
            return columns.Remove(columns.Length - 1);
        }

        public string MakeDatasetsExtraColumnsWithDataType()
        {
            string columns = "";
            foreach (string column in ColumnNames())
            {
                columns += "\n" + column + "\t\t" + GetPostgresColumnType(column) + ",";
            }

            // Return without the last comma ","
            return columns.Remove(columns.Length - 1);
        }

        private string GetPostgresColumnType(string column)
        {
            if (column == "Date")
                return "DATE";
            if (column == "Value")
                return "NUMERIC";
            if (column == "Open" ||
                column == "High" ||
                column == "Low" ||
                column == "Close" ||
                column == "Volume" ||
                column == "ExDividend" ||
                column == "SplitRatio" ||
                column == "AdjOpen" ||
                column == "AdjHigh" ||
                column == "AdjLow" ||
                column == "AdjClose" ||
                column == "AdjustedClose" ||
                column == "AdjVolume" ||
                column == "Last" ||
                column == "Settle" ||
                column == "PrevDayOpenInterest"
                )
                return "NUMERIC";

            return "TEXT";
        }

        private static string GetPostgresColumnType(dynamic data, string column)
        {
            Type type = data?.GetType() == null ? null : data?.GetType();

            if (type == null)
                return "TEXT";
            if (IsNumericType(type))
                return "NUMERIC";
            if (Type.GetTypeCode(type.GetType()) == TypeCode.Boolean)
                return "BOOL";
            if (Type.GetTypeCode(type.GetType()) == TypeCode.String
                && column.ToLower() == "date")
                return "DATE";
            if (Type.GetTypeCode(type.GetType()) == TypeCode.String
                && column.ToLower() != "date")
                return "TEXT";

            else
                return "TEXT";
        }

        public static bool IsNumericType(Type o)
        {
            switch (Type.GetTypeCode(o.GetType()))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }
    }
}
