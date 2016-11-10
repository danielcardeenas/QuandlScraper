using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Model.Quandl
{
    public class QuandlColumn
    {
        public string Name { get; set; }
        public string Type { get; set; }

        public string GetPostgresType()
        {
            string type = Type.ToLower();
            if (type == "string")
                return "text";
            else if (type == "date")
                return "date";
            else if (type == "timestamp" || type == "time")
                return "timestamp";
            else if (type == "integer" || type == "int")
                return "numeric";
            else if (type.Contains("bigdecimal"))
                return "numeric";
            else if (type == "double")
                return "numeric";
            else if (type == "biginteger")
                return "numeric";
            else if (type.Contains("boolean") || type == "bool")
                return "boolean";
            else if (type == "long")
                return "numeric";

            return "text";
        }
    }
}
