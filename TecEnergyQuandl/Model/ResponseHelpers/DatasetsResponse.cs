using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;

namespace TecEnergyQuandl.Model.ResponseHelpers
{
    public class DatasetsResponse
    {
        public MetaObject Meta { get; set; }
        public List<QuandlDataset> Datasets { get; set; }
    }
}
