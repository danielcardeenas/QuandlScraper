using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model.Quandl
{
    public class QuandlDataset
    {
        public long Id { get; set; }
        public string DatasetCode { get; set; }
        public string DatabaseCode { get; set; }

        private string name;
        public string Name
        {
            get { return name; }
            set { name = value.Replace("'", ""); }
        }

        private string description;
        public string Description
        {
            get { return description; }
            set { description = value.Replace("'", ""); }
        }

        //public DateTime RefreshedAt { get; set; }
        //public DateTime? LastFetchedDate { get; set; }
        public DateTime? NewestAvailableDate { get; set; }
        public DateTime? OldestAvailableDate { get; set; }

        private List<string> columnNames;
        public List<string> ColumnNames
        {
            get { return columnNames; }
            set
            {
                // Remove special characters
                columnNames = value.Select(c => c
                    .Replace("'", "")
                    .Replace("-", "")
                    .Replace(".", "")
                    .Replace(" ", ""))
                    .ToList();
            }
        }

        public string Frequency { get; set; }
        public string Type { get; set; }
        public bool Premium { get; set; }
        public long DatabaseId { get; set; }
        public bool Import { get; set; }

        public static QuandlDataset MakeQuandlDataset(NpgsqlDataReader row)
        {
            var dataset = new QuandlDataset()
            {
                Id = (long)row["id"],
                DatasetCode = (string)row["datasetcode"],
                DatabaseCode = (string)row["databasecode"],
                Name = (string)row["name"],
                Description = (string)row["description"],
                NewestAvailableDate = row.GetDateTime(row.GetOrdinal("newestavailabledate")),
                OldestAvailableDate = row.GetDateTime(row.GetOrdinal("oldestavailabledate")),
                //NewestAvailableDate = (DateTime)row["newestavailabledate"],
                //OldestAvailableDate = (DateTime)row["oldestavailabledate"],
                ColumnNames = row["columnnames"].ToString()
                                                .Split(',')
                                                .Select(x => x.Trim())
                                                .Where(x => !string.IsNullOrWhiteSpace(x))
                                                .ToList(),
                Frequency = (string)row["frequency"],
                Type = (string)row["type"],
                Premium = (bool)row["premium"],
                DatabaseId = (long)row["databaseid"],
                Import = (bool)row["import"]
            };

            return dataset;
        }

        public static string GetColumnsForQuery()
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
                                DatabaseId,
                                Import";

            return columns;
        }

        public static string GetColumnsForQuerySuffixed(string suffix)
        {
            string columns = @" [].Id,
                                [].DatasetCode,
                                [].DatabaseCode,
                                [].Name,
                                [].Description,
                                [].NewestAvailableDate,
                                [].OldestAvailableDate,
                                [].ColumnNames,
                                [].Frequency,
                                [].Type,
                                [].Premium,
                                [].DatabaseId,
                                [].Import";

            return columns.Replace("[]", suffix);
        }
    }
}
