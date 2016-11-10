using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;

namespace TecEnergyQuandl.Model.ResponseHelpers
{
    public class DatatableResponse
    {
        public QuandlDatatable Datatable { get; set; }
        public DatatableMeta Meta { get; set; }
    }

    public class DatatableMeta
    {
        public string NextCursorId { get; set; }
    }
}
