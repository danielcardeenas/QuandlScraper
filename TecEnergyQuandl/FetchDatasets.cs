using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace TecEnergyQuandl
{
    public static class FetchDatasets
    {
        public static void BeginDownloadDatasets()
        {
            using (WebClient client = new WebClient())
            {
                // Download first page and check meta
                Console.WriteLine("Fetching datasets\n---------------------------------------");
            }
        }
    }
}
