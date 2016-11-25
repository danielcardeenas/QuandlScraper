using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model.Postgres
{
    public class TableDescription
    {
        public Dictionary<string, string> Dict { get; set; }

        public TableDescription(NpgsqlDataReader row)
        {
        }
    }
}
