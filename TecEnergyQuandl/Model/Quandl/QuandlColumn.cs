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

        public string GetPostgreswFormatForColumn(int position)
        {
            if (IsText())
                return "'{" + position + "}'";
            else if (IsNumeric())
                return "cast(coalesce(nullif('{" + position + "}',''),null) as float)";
            else if (GetPostgresType().ToLower() == "timestamp")
                return "to_timestamp('{" + position + "}', 'YYYY-MM-DD hh24:mi:ss')";
            else if (GetPostgresType().ToLower() == "date")
                return "to_date('{" + position + "}', 'YYYY-MM-DD')";

            // Default is text
            else
                return "'{" + position + "}'";
        }

        public bool IsText()
        {
            if (GetPostgresType().ToLower() == "text")
                return true;
            else
                return false;
        }

        public bool IsNumeric()
        {
            if (GetPostgresType().ToLower() == "numeric")
                return true;
            else
                return false;
        }
    }
}
