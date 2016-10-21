using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model.Quandl
{
    public class QuandlDatasetGroup
    {
        public string DatabaseCode { get; set; }
        public List<QuandlDataset> Datasets { get; set; }

        private List<string> queries;
        // Return extra columns
        // Ex. For WIKI:
        //  Date, Vol, High, Low, etc..
        //
        // Each Database has its own extra columns
        public List<string> ColumnNames()
        {
            return Datasets.ElementAt(0).ColumnNames;
        }   

        // Not ready
        public string MakeInsertQuery()
        {
            string query = "INSERT INTO quandl.datasets (" + QuandlDataset.GetColumnsForQuery() + ") VALUES ";

            var last = Datasets.Last();
            foreach (QuandlDataset item in Datasets)
            {
                query += String.Format(@"({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', {10}, {11}, {12})",
                                    item.Id, item.DatasetCode, item.DatabaseCode, item.Name, item.Description, // 0 - 4
                                    item.NewestAvailableDate.ToString("yyyy/MM/dd"), item.OldestAvailableDate.ToString("yyyy/MM/dd"), // 5 - 6
                                    string.Join(",", item.ColumnNames), // 7 
                                    item.Frequency, item.Type, // 8 - 9
                                    item.Premium, item.DatabaseId, item.Import); // 10 - 12

                if (item != last)
                    query += ",\n";
            }

            query += "\nON CONFLICT(id) DO NOTHING";
            return query;
        }

        public string MakeInsertDataQuery()
        {
            string query = "INSERT INTO quandl." + DatabaseCode + "(" + GetColumnsForInsertDataQuery() + ") VALUES";
            int elementsPerThread = 500;
            queries = new List<string>();
            var tasks = new List<Task>();
            foreach (QuandlDatasetData item in Datasets)
            {
                tasks.AddRange(CreateQueryThreads(item, elementsPerThread));
            }

            // Wait for all the threads to complete
            Task.WaitAll(tasks.ToArray());

            // Join all the querys
            string values = String.Join("\n", queries);

            // Remove last comma ","
            values = values.Remove(values.Length - 1);

            // Join "insert into... values" + "(...)"
            query += values;
            query += "\nON CONFLICT(id) DO NOTHING";

            return query;
        }

        private int GetThreadsNeeded(int dataCount, int elementsPerThread)
        {
            return (int)Math.Ceiling(((double)dataCount / (double)elementsPerThread));
        }

        private Task[] CreateQueryThreads(QuandlDatasetData item, int elementsPerThread)
        {
            var tasks = new List<Task>();
            int threadsNeeded = GetThreadsNeeded(item.Data.Count, elementsPerThread);

            int remainingElements = item.Data.Count;
            int from = 0;
            for (int i = 0; i < threadsNeeded; i++)
            {
                int fromIndex = from;
                int toIndex = fromIndex + elementsPerThread;
                tasks.Add(Task.Factory.StartNew(() => CreatePartialQuery(item, fromIndex, toIndex)));

                from += elementsPerThread;
            }

            return tasks.ToArray();
        }

        private void CreatePartialQuery(QuandlDatasetData dataset, int from, int to)
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
            if (GetPostgresColumnType(column) == "TEXT" ||
                GetPostgresColumnType(column) == "DATE")
                return "'{" + number + "}'";
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
                column == "AdjVolume"
                )
                return "NUMERIC";

            return "TEXT";

        }
    }
}
