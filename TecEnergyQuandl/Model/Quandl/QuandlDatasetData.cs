using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model.Quandl
{
    public class QuandlDatasetData : QuandlDataset
    {
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public string Transform { get; set; }
        public string Collapse { get; set; }
        public List<object[]> Data { get; set; }

        public void SetBaseDataset(QuandlDataset dataset)
        {
            Id = dataset.Id;
            DatabaseCode = dataset.DatabaseCode;
            DatasetCode = dataset.DatasetCode;
            Name = dataset.Name;
            Description = dataset.Description;
            NewestAvailableDate = dataset.NewestAvailableDate;
            OldestAvailableDate = dataset.OldestAvailableDate;

            // Ignore column names from database
            // Use the ones fetched from quandl instead
            // Uncomment to use database ones
            //ColumnNames = dataset.ColumnNames;

            Frequency = dataset.Frequency;
            Type = dataset.Type;
            Premium = dataset.Premium;
            DatabaseId = dataset.DatabaseId;
            Import = dataset.Import;
        }

        public string GetColumnsForInsertDataQuery()
        {
            string columns = @" DatasetCode,
                                DatabaseCode,
                                Name,
                                Transform,
                                DatabaseId,
                                date_insert," +
                                // Column names [specific data]
                                MakeDatasetsExtraColumns();

            return columns;
        }

        private string MakeDatasetsExtraColumns()
        {
            string columns = "";
            foreach (string column in ColumnNames)
            {
                columns += "\n" + column + ",";
            }

            // Return without the last comma ","
            return columns.Remove(columns.Length - 1);
        }
    }
}
