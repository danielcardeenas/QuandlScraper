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
        public DateTime NewestAvailableDate { get; set; }
        public DateTime OldestAvailableDate { get; set; }
        public List<string> ColumnNames { get; set; }
        public string Frequency { get; set; }
        public string Type { get; set; }
        public bool Premium { get; set; }
        public long DatabaseId { get; set; }
    }
}
