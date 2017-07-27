using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl.Utils
{
    public class Constants
    {
        public static string API_KEY = "YOUR_API_KEY";
        public static string DATABASES_URL = "https://www.quandl.com/api/v3/databases.json";

        // Postgres stuff
        public static string USER = "postgres";
        public static string PASSWORD = "postgres";
        public static string BASE_CONNECTION_STRING = "Host=localhost;" +
                                                 "Username=" + USER + ";" + 
                                                 "Password=" + PASSWORD + ";";

        public static string CONNECTION_STRING = BASE_CONNECTION_STRING + "Database=quandl";

        public static string[] DATATABLES = {
            "ZACKS/EB",
            "ZACKS/MT",
            "ZACKS/FC",
            "ZACKS/FR",
            "ZACKS/MKTV",
            "ZACKS/SHRS",
            "ZACKS/HDM",
            "ZACKS/CP"
        };
    }
}
