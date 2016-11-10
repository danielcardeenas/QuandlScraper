using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model.Quandl
{
    public class QuandlDatatable
    {
        public string Name { get; set; }
        public bool Import { get; set; }

        public List<object> Data { get; set; }
        public List<QuandlColumn> Columns { get; set; }

        public static QuandlDatatable MakeQuandlDatabase(NpgsqlDataReader row)
        {
            var datatable = new QuandlDatatable()
            {
                Name = (string)row["name"],
                //Import = (bool)row["import"]
            };

            return datatable;
        }

        public string MakeExtraColumnsWithDataType()
        {
            string columns = "";
            foreach (QuandlColumn column in Columns)
            {
                columns += "\n" + column.Name + "\t\t" + column.GetPostgresType() + ",";
            }

            // Return without the last comma ","
            return columns.Remove(columns.Length - 1);
        }
    }
}
