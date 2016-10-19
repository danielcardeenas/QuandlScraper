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

        public List<string> ColumnNames()
        {
            return Datasets.ElementAt(0).ColumnNames;
        }

        // Not ready
        public string MakeInsertQuery()
        {
            string query = "INSERT INTO quandl.datasets (" + GetColumnsForQuery() + ") VALUES ";

            var last = Datasets.Last();
            foreach (QuandlDataset item in Datasets)
            {
                query += String.Format(@"({0}, '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', {10}, {11})",
                                    item.Id, item.DatasetCode, item.DatabaseCode, item.Name, item.Description, // 0 - 4
                                    item.NewestAvailableDate.ToString("yyyy/MM/dd"), item.OldestAvailableDate.ToString("yyyy/MM/dd"), // 5 - 6
                                    string.Join(",", item.ColumnNames), // 7 
                                    item.Frequency, item.Type, // 8 - 9
                                    item.Premium, item.DatabaseId); // 10 - 11

                if (item != last)
                    query += ",\n";
            }

            query += "\nON CONFLICT(id) DO NOTHING";
            return query;

        }

        // Not ready
        public string MakeInsertDataQuery()
        {
            string query = "INSERT INTO quandl." + DatabaseCode + "(" + GetColumnsForDataQuery() + ") VALUES";
            //foreach (QuandlDataset item in Datasets)
            //{
            //    query += String.Format(@"({0}, '{1}', '{2}', '{3}', {4}, {5}, {6}, '{7}', {8})",
            //                        item.Id, item.Name, item.DatabaseCode, item.Description, item.DatasetsCount, item.Downloads, item.Premium, item.Image, item.Favorite);

            //    if (item != last)
            //        query += ",\n";
            //}

            //query += "\nON CONFLICT(id) DO NOTHING";
            return query;

        }

        private string GetColumnsForQuery()
        {
            string columns = @" Id,
                                DatasetCode,
                                DatabaseCode,
                                Name,
                                Description,
                                NewestAvailableDate,
                                OldestAvailableDate,
                                ColumnNames,
                                Frequency,
                                Type,
                                Premium,
                                DatabaseId";

            return columns;
        }

        private string GetColumnsForDataQuery()
        {
            string columns = @" Id,
                                DatasetCode,
                                DatabaseCode,
                                Name,
                                Description,
                                NewestAvailableDate,
                                OldestAvailableDate,
                                Frequency,
                                Type,
                                Premium,
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
                column == "Ex-Dividend" ||
                column == "Split Ratio" ||
                column == "Adj. Open" ||
                column == "Adj. High" ||
                column == "Adj. Low" ||
                column == "Adj. Close" ||
                column == "Adjusted Close" ||
                column == "Adj. Volume"
                )
                return "NUMERIC";

            return "TEXT";

        }
    }
}
