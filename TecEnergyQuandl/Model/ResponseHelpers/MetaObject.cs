using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model.ResponseHelpers
{
    public class MetaObject
    {
        public int PerPage { get; set; }
        public string Query { get; set; }
        public int CurrentPage { get; set; }
        public int? PrevPage { get; set; }
        public int TotalPages { get; set; }
        public int TotalCount { get; set; }
        public int? NextPage { get; set; }
        public int CurrentFirstItem { get; set; }
        public int CurrentLastItem { get; set; }
    }
}