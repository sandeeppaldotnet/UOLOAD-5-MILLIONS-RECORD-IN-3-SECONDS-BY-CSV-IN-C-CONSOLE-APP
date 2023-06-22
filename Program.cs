using LumenWorks.Framework.IO.Csv;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace upload_5_million_record_of_csv_file_by_c_sharp_dotnet
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            ExportAndSaveDataByLumen();
            stopwatch.Stop();
            Console.WriteLine($"Time ellapsed {stopwatch.ElapsedMilliseconds / 1000}");
            Console.ReadLine();
        }
        public static void ExportAndSaveDataByLumen()
        {
            List<Tuple<string, string, string>> listA = new List<Tuple<string, string, string>>();
            using (CsvReader csv = new CsvReader(new StreamReader(@"C:\Users\B R S\Downloads\5mdata.csv"), true))
            {
                while (csv.ReadNextRecord())
                {
                    listA.Add(new Tuple<string, string, string>(csv[0], csv[1], csv[11]));
                }
            }
            var top10HigestRevenueSalesRecords = from salesrec in listA
                                                 orderby salesrec.Item3
                                                 select salesrec;

            //PRINT  test  tests
            int i = 0;
            foreach (var item in top10HigestRevenueSalesRecords)
            {
                i++;
                Console.WriteLine($"{i}:{item.Item1} - {item.Item2} - {item.Item3}");
                //add(item.Item1, item.Item2, item.Item3);
            }
        }
        public static void ExportAndSaveDataByLoop()
        {
            //LOAD    
            //Created a temporary dataset to hold the records    
            List<Tuple<string, string, string>> listA = new List<Tuple<string, string, string>>();
            using (var reader = new StreamReader(@"C:\Users\B R S\Downloads\5mdata.csv"))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');
                    listA.Add(new Tuple<string, string, string>(values[0], values[1], values[11]));
                }
            }
            //PROCESS    
            //var top10HigestRevenueSalesRecords = from salesrec in listA.Skip(0).Take(10)
            // orderby salesrec.Item3
            // select salesrec;


            var top10HigestRevenueSalesRecords = from salesrec in listA
                                                 orderby salesrec.Item3
                                                 select salesrec;

            //PRINT    
            int i = 0;
            foreach (var item in top10HigestRevenueSalesRecords)
            {
                //i++;
                //Console.WriteLine($"{i}:{item.Item1} - {item.Item2} - {item.Item3}");
                add(item.Item1, item.Item2, item.Item3);
            }
        }

        public static void add(string Item1, string Item2, string Item3)
        {
            string con = "Data Source=DESKTOP-10LA8I4\\SQLEXPRESS;Initial Catalog = TestDB;Integrated Security=True";
            SqlConnection conn = new SqlConnection(con);

            try
            {



                SqlCommand cmd = new SqlCommand("insertDATA", conn);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@Item1", Item1);
                cmd.Parameters.AddWithValue("@Item2", Item2);
                cmd.Parameters.AddWithValue("@Item3", Item3);
                conn.Open();
                int i = cmd.ExecuteNonQuery();
                conn.Close();

            }
            catch (Exception e)
            {

            }
        }
    }
}
