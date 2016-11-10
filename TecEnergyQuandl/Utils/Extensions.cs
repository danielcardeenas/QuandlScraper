using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TecEnergyQuandl.Model.Quandl;

namespace TecEnergyQuandl.Utils
{
    public static class Extensions
    {
        public static object[] PrepareForPostgres(this object[] data, List<QuandlColumn> columns)
        {
            for(int i = 0; i < columns.Count; i++)
            {
                QuandlColumn column = columns.ElementAt(i);
                if (column.IsText())
                {
                    // Escape the single quote
                    if (data[i] != null)
                        data[i] = data[i].ToString().Replace("'", "''");
                    else
                        data[i] = "";
                }
            }
            return data;
        }
    }
}
