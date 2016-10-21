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
            ColumnNames = dataset.ColumnNames;
            Frequency = dataset.Frequency;
            Type = dataset.Type;
            Premium = dataset.Premium;
            DatabaseId = dataset.DatabaseId;
            Import = dataset.Import;
        }
    }
}
