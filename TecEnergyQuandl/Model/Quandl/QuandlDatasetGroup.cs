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

            var lastDataset = (QuandlDatasetData)Datasets.Last();
            var lastDataObject = lastDataset.Data.Last();
            foreach (QuandlDatasetData item in Datasets)
            {
                foreach (object[] data in item.Data)
                {
                    // Base insert
                    query += String.Format(@"('{0}', '{1}', '{2}', '{3}', {4}",
                                    item.DatasetCode, item.DatabaseCode, item.Name, // 0 - 2
                                    item.Transform, // 3
                                    item.DatabaseId); // 4


                    // Extra columns
                    query += String.Format(FormatExtraColumns(0) + " )",
                                    data);



                    if (item != lastDataset || data != lastDataObject)
                        query += ",\n";
                }
            }

            query += "\nON CONFLICT(id) DO NOTHING";
            return query;

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
