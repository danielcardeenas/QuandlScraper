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

        private List<string> columnNames;
        private Dictionary<string, object> columnsDict = new Dictionary<string, object>();

        //private List<string> queries;
        private string queryFilePath;

        // Return extra columns
        // Ex. For WIKI:
        //  Date, Vol, High, Low, etc..
        //
        // Each Database has its own extra columns
        public List<string> ColumnNames()
        {
            if (columnNames == null)
            {
                columnNames = new List<string>();
                foreach (var dataset in Datasets)
                    columnNames.AddRange(dataset.ColumnNames.Except(columnNames));
            }

            return columnNames;
        }

        // Detects primary keys
        public string[] PrimaryKeys()
        {
            List<string> primaryKeys = new List<string>();

            if (HasColumnDate())
                primaryKeys.Add("date");

            primaryKeys.Add("datasetcode");

            return primaryKeys.ToArray();
        }

        public string MakePrimaryKeysForCreate()
        {
            string query = "\ndate\t\tdate,";

            // Return without the last comma ","
            return query.Remove(query.Length - 1);
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
            //queries = new List<string>();

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
            //queries = new List<string>();

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
        public void MakeInsertDataQuery()
        {
            // Data elements to be formated for each thread
            int elementsPerThread = 500;

            // Init where all queries generated will belong
            //queries = new List<string>();

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
                return;

            // Wait for all the threads to complete
            Task.WaitAll(tasks.ToArray());
        }

        public bool HasColumnDate()
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
            string query = @"WITH data(" + dataset.GetColumnsForInsertDataQuery() + @") as ( values";

            if (to > dataset.Data.Count)
                to = dataset.Data.Count;

            for (int i = from; i < to; i++)
            {
                // Current
                object[] data = dataset.Data[i];

                // Base insert
                query += String.Format(@"('{0}', '{1}', '{2}', '{3}', {4}",
                                dataset.DatasetCode, dataset.DatabaseCode, dataset.Name, // 0 - 2
                                dataset.Transform, // 3
                                dataset.DatabaseId); // 4

                // Mod data
                MakeDateTimeStamp(ref data);

                // Extra columns
                string format = FormatExtraColumns(0, dataset);
                query += String.Format(FormatExtraColumns(0, dataset) + " ),",
                                data);
            }

            // Remove last comma ","
            query = query.Remove(query.Length - 1);

            query += ")"; // Close (values ... ) 
            query += "\nINSERT INTO quandl." + DatabaseCode + "(" + dataset.GetColumnsForInsertDataQuery() + ")" +
                     " SELECT " + dataset.GetColumnsForInsertDataQuery() +
                     " FROM data";

            if (HasColumnDate())
            {
                query += @" WHERE NOT EXISTS (
                            SELECT 1 FROM quandl." + DatabaseCode + @" ds 
                                WHERE ds.date = data.date AND
                                      ds.datasetcode = data.datasetcode
                            )";
            }

            // Execute query
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
                        using (var mutex = new Mutex(false, "SHARED_INSERT_DATASETS"))
                        {
                            mutex.WaitOne();
                            // Write
                            // ===============================================
                            using (StreamWriter sw = File.AppendText("log.txt"))
                            {
                                Utils.ConsoleInformer.Inform("Some unexpected stuff happened. See the log for more info");
                                Utils.Helpers.Log("Failed to insert data chunk.\n-------------------\nStart query:\n" + query + "\n-------------------\nEnd query\n", 
                                    ex.Message, sw);
                            }

                            // End process file
                            // ===============================================
                            mutex.ReleaseMutex();
                        }

                        //Helpers.ExitWithError(ex.Message);
                    }

                    // Close connection
                    // ===============================================================
                    conn.Close();
                }
            }
        }

        private void MakeDateTimeStamp(ref object[] data)
        {
            var dateIndex = ColumnNames().FindIndex(a => a.ToLower() == "date");
            DateTime myDate = DateTime.Parse(data[dateIndex].ToString());
            data[dateIndex] = myDate.ToString("yyyy-MM-dd HH:mm:ss");
        }

        private string FormatExtraColumns(int fromNumber, QuandlDatasetData dataset)
        {
            string extraColumns = "";

            for (int i = 0; i < dataset.ColumnNames.Count; i++)
            {
                string column = dataset.ColumnNames.ElementAt(i);

                // Here maybe should check if the value is null
                // If its null then keep checking to the next data[] element
                // F. ex: ((QuandlDatasetData)Datasets.ElementAt(0)).Data.ElementAt(1...inf).ElementAt(i)
                // Until it get a non null value
                //dynamic data = ((QuandlDatasetData)Datasets.ElementAt(0)).Data.ElementAt(0).ElementAt(i);

                // Already done what above comment says
                dynamic data = GetSampleDataOnColumn(column);

                extraColumns += ", " + PrepareExtraColumnFormated(data, column, fromNumber);
                fromNumber++;
            }

            return extraColumns;
        }

        private string PrepareExtraColumnFormated(dynamic data, string column, int number)
        {
            if (GetPostgresColumnType(data, column).ToLower() == "text")
                return "'{" + number + "}'";
            else if (GetPostgresColumnType(data, column).ToLower() == "timestamp")
                return "to_timestamp('{" + number + "}', 'YYYY-MM-DD hh24:mi:ss')";
            else if (GetPostgresColumnType(data, column).ToLower() == "date")
                return "to_date('{" + number + "}', 'YYYY-MM-DD')";
            else
                return "cast(coalesce(nullif('{" + number + "}',''),null) as float)";
        }

        [Obsolete("Use the QuandlDatasetData method instead")]
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

        [Obsolete("Use the QuandlDatasetData method instead")]
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
            for (int i = 0; i < ColumnNames().Count; i++)
            {
                string column = ColumnNames().ElementAt(i);

                // Here maybe should check if the value is null
                // If its null then keep checking to the next data[] element
                // F. ex: ((QuandlDatasetData)Datasets.ElementAt(0)).Data.ElementAt(1...inf).ElementAt(i)
                // Until it get a non null value
                //dynamic data = ((QuandlDatasetData)Datasets.ElementAt(0)).Data.ElementAt(0).ElementAt(i);

                dynamic data = GetSampleDataOnColumn(column);
                columns += "\n" + column + "\t\t" + GetPostgresColumnType(data, column) + ",";
            }

            // Return without the last comma ","
            return columns.Remove(columns.Length - 1);
        }

        // Find a dataset with the same column name and check its data type
        private object GetSampleDataOnColumn(string column)
        {
            //QuandlDatasetData datasetDataSample;
            foreach (var datasetData in Datasets)
            {
                // Every dataset in this group has the same data structure
                QuandlDatasetData datasetDataSample;

                try { datasetDataSample = (QuandlDatasetData)datasetData; }
                catch (Exception ex) { continue; }

                // Check if this data is not null and if it has data
                if (datasetDataSample != null && datasetDataSample.Data.Count > 0)
                {
                    // This dataset has the column im looking for?
                    if (datasetDataSample.ColumnNames.Any(col => col == column))
                    {
                        // Get the index of this column in the dataset
                        var index = datasetDataSample.ColumnNames.FindIndex(c => c == column);

                        foreach (object[] data in datasetDataSample.Data)
                        {
                            // Verify if it contains and if the value is not null
                            if (data.Count() < index || data.ElementAt(index) == null)
                                continue;

                            return data.ElementAt(index);
                        }
                    }
                    else
                    {
                        continue;
                    }
                }
            }

            // Return text by default
            return "";
        }

        private object GetSampleDataOnIndex(int index, QuandlDatasetData dataset)
        {
            // Check if this data is not null and if it has data
            if (dataset == null || dataset.Data.Count > 0)
            {
                foreach (object[] data in dataset.Data)
                {
                    // Verify if it contains and if the value is not null
                    if (data.Count() < index || data.ElementAt(index) == null)
                        continue;

                    return data.ElementAt(index);
                }
            }

            // Return text by default
            return "";
        }

        //private string GetPostgresColumnType(string column)
        //{
        //    if (column.ToLower() == "date")
        //        return "TIMESTAMP";
        //    if (column.ToLower() == "value")
        //        return "NUMERIC";
        //    if (column.ToLower() == "open" ||
        //        column.ToLower() == "high" ||
        //        column.ToLower() == "low" ||
        //        column.ToLower() == "close" ||
        //        column.ToLower() == "volume" ||
        //        column.ToLower() == "exdividend" ||
        //        column.ToLower() == "splitratio" ||
        //        column.ToLower() == "adjopen" ||
        //        column.ToLower() == "adjhigh" ||
        //        column.ToLower() == "adjlow" ||
        //        column.ToLower() == "adjclose" ||
        //        column.ToLower() == "adjustedclose" ||
        //        column.ToLower() == "adjvolume" ||
        //        column.ToLower() == "last" ||
        //        column.ToLower() == "settle" ||
        //        column.ToLower() == "prevdayopeninterest"
        //        )
        //        return "NUMERIC";

        //    return "TEXT";
        //}

        private static string GetPostgresColumnType(object data, string column)
        {
            Type type = data?.GetType() == null ? null : data?.GetType();

            if (type == null)
                return "TEXT";
            if (IsNumericType(type))
                return "NUMERIC";
            if (Type.GetTypeCode(type) == TypeCode.Boolean)
                return "BOOL";
            if (Type.GetTypeCode(type) == TypeCode.String
                && column.ToLower() == "date")
                return "DATE";
            if (Type.GetTypeCode(type) == TypeCode.String
                && column.ToLower() == "timestamp")
                return "TIMESTAMP";
            if (Type.GetTypeCode(type) == TypeCode.String
                && column.ToLower() != "date")
                return "TEXT";

            else
                return "TEXT";
        }

        public static bool IsNumericType(Type o)
        {
            switch (Type.GetTypeCode(o))
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
