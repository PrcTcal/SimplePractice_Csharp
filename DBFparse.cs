using System;
using System.Data;
using System.Data.OleDb;

namespace DBFconnection{
    class connector{

        public void connect(string fileName){
            string strConn = "Provider=Microsoft.Jet.OLEDB.4.0; Data Source=./dbf; User ID=admin; Password=123";
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
        
    }
}