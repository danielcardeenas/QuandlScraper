using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model
{
    public class QuandlDatabase
    {
        public long Id { get; set; }
        private string name;
        public string Name
        {
            get { return name; }
            set { name = value.Replace("'", ""); }
        }

        public string DatabaseCode { get; set; }
        private string description;
        public string Description
        {
            get { return description; }
            set { description = value.Replace("'", ""); }
        }

        public long DatasetsCount { get; set; }
        public long Downloads { get; set; }
        public bool Premium { get; set; }
        public string Image { get; set; }
        public bool Favorite { get; set; }
        public bool Import { get; set; }
    }
}
