using System;
using System.Data;
using System.Data.OleDb;
using DbfDataReader;

namespace DBFconnection{
    class dbf{

        public static void connect(string fileName){
            string strConn = "Provider=Microsoft.Jet.OLEDB.4.0; Data Source=./dbf;";
            OleDbConnection conn = new OleDbConnection(strConn);

            conn.Open();
            Console.WriteLine("Database : " + conn.Database);
            Console.WriteLine("DataSource : " + conn.DataSource);
            Console.WriteLine("Version : " + conn.ServerVersion);
            Console.WriteLine("State : " + conn.State);

            conn.Close();
            Console.WriteLine("State : " + conn.State);

            string sql = "select * from " + fileName;
            OleDbCommand cmd = new OleDbCommand(sql, conn);
            conn.Open();
            DataSet ds = new DataSet();
            OleDbDataAdapter oda = new OleDbDataAdapter(cmd);
            oda.Fill(ds);
            Console.WriteLine(ds.Tables);
            conn.Close();
        }

        public static void reader(){
            string dbfPath = "./dbf/dbase_83.dbf";
            /*
            using (var dbfTable = new DbfTable(dbfPath, EncodingProvider.UTF8))
            {
                var dbfRecord = new DbfRecord(dbfTable);
                while (dbfTable.Read(dbfRecord))
                {
                    for(int i = 0 ; i < dbfRecord.Values.Count - 1 ; i++){
                        Console.WriteLine(dbfRecord.Values[i].ToString() + "\t" + dbfRecord.Values[i].GetType().ToString());
                    }
                    //Console.WriteLine(dbfRecord.Values[5].ToString() == null);
                }
            }
            */
            var options = new DbfDataReaderOptions{
                SkipDeletedRecords = true,
                Encoding = EncodingProvider.UTF8
            };
            object[] objs = new object[6];
            int quant = 0;
            using(var dbfReader = new DbfDataReader.DbfDataReader(dbfPath, options)){
                while(dbfReader.Read()){
                    string[] names = new string[dbfReader.FieldCount];
                    for(int i = 0 ; i < dbfReader.FieldCount ; i++){
                        names[i] = dbfReader.GetName(i);
                        if(dbfReader.DbfRecord.Values[i].GetType() != typeof(DbfDataReader.DbfValueMemo)){
                            Console.WriteLine(names[i] + " : " + dbfReader.DbfRecord.Values[i].ToString());
                        }
                    }
                    Console.WriteLine("=======================================");
                }
            }
            
        }
        
    }
}