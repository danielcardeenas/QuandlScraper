using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Utils;

namespace TecEnergyQuandl.Model.Quandl
{
    public class QuandlDatatable
    {
        public string Name { get; set; }
        public bool Import { get; set; }

        public List<object[]> Data { get; set; }
        public List<QuandlColumn> Columns { get; set; }

        public static QuandlDatatable MakeQuandlDatabase(NpgsqlDataReader row)
        {
            var datatable = new QuandlDatatable()
            {
                Name = (string)row["name"],
                //Import = (bool)row["import"]
            };

            return datatable;
        }

        public string MakeExtraColumnsWithDataType()
        {
            string columns = "";
            foreach (QuandlColumn column in Columns)
            {
                columns += "\n" + column.Name + "\t\t" + column.GetPostgresType() + ",";
            }

            // Return without the last comma ","
            return columns.Remove(columns.Length - 1);
        }

        public void MakeInsertQuery()
        {
            // Data elements to be formated for each thread
            int elementsPerThread = 500;

            // Init taks
            var tasks = new List<Task>();

            // Create query only if needed
            if (Data.Count > 0)
                tasks.AddRange(CreateQueryThreads(elementsPerThread));
            else
                Utils.ConsoleInformer.Inform("Datatable [" + Name + "] is already in its last version");

            // If nothing to do just skip
            if (tasks.Count <= 0)
                return;

            // Wait for all the threads to complete
            Task.WaitAll(tasks.ToArray());
        }

        private Task[] CreateQueryThreads(int elementsPerThread)
        {
            var tasks = new List<Task>();
            int threadsNeeded = GetThreadsNeeded(Data.Count, elementsPerThread);

            int remainingElements = Data.Count;
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

        private void CreatePartialQuery(int from, int to)
        {
            if (to > Data.Count)
                to = Data.Count;

            string query = @"WITH data(" + GetColumnsCommaSeparated() + @", date_insert) as ( values";
            //string query = "";
            for (int i = from; i < to; i++)
            {
                // Current
                object[] data = Data[i];

                data = data.PrepareForPostgres(Columns);

                // Base insert
                query += String.Format("(" + GetColumnsFormatted() + ", date_trunc('second',current_timestamp))", data);
                query += ",";
            }

            // Remove last comma ","
            query = query.Remove(query.Length - 1);
            query += ")\n"; // Close (values ... ) 

            query += @"INSERT INTO quandl.""" + Name + @""" (" + GetColumnsCommaSeparated() + ", date_insert)" +
                    " SELECT " + GetColumnsCommaSeparated() + ", date_insert" +
                    " FROM data";
                    //" WHERE NOT EXISTS (SELECT 1 FROM quandl.datasets ds WHERE ds.Id = data.Id)";

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

        private string GetColumnsFormatted()
        {
            string format = "";
            for (int i = 0; i < Columns.Count; i++)
            {
                QuandlColumn column = Columns.ElementAt(i);
                format += column.GetPostgreswFormatForColumn(i);

                // If not last, separate with comma
                if (i != Columns.Count - 1)
                    format += ", ";
            }

            return format;
        }

        private string GetColumnsCommaSeparated()
        {
            string query = "";
            var last = Columns.Last();
            foreach (QuandlColumn column in Columns)
            {
                query += column.Name;

                if (column != last)
                    query += ", ";
            }

            return query;
        }

        private int GetThreadsNeeded(int dataCount, int elementsPerThread)
        {
            return (int)Math.Ceiling(((double)dataCount / (double)elementsPerThread));
        }
    }
}
