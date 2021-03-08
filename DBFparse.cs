using System;
using System.IO;
using DbfDataReader;
using Newtonsoft.Json.Linq;


namespace DBFconnection
{
    class dbf
    {

        public static void reader()
        {
            string dbfPath = "./dbf/dbase_83.dbf";
            var options = new DbfDataReaderOptions
            {
                SkipDeletedRecords = true,
                Encoding = EncodingProvider.UTF8
            };

            JArray rs = new JArray();
            using (var dbfReader = new DbfDataReader.DbfDataReader(dbfPath, options))
            {
                while (dbfReader.Read())
                {
                    JObject json_object = new JObject();
                    for (int i = 0; i < dbfReader.FieldCount; i++)
                    {
                        if (dbfReader.DbfRecord.Values[i].GetType() != typeof(DbfDataReader.DbfValueMemo))
                        {
                            if(dbfReader.DbfRecord.Values[i].GetType() == typeof(DbfDataReader.DbfValueInt))
                            {
                                json_object.Add(dbfReader.GetName(i), Convert.ToInt32(dbfReader.DbfRecord.Values[i].ToString()));
                            }
                            else if(dbfReader.DbfRecord.Values[i].GetType() == typeof(DbfDataReader.DbfValueDecimal))
                            {
                                json_object.Add(dbfReader.GetName(i), Convert.ToDecimal(dbfReader.DbfRecord.Values[i].ToString()));
                            }
                            else if(dbfReader.DbfRecord.Values[i].GetType() == typeof(DbfDataReader.DbfValueBoolean))
                            {
                                json_object.Add(dbfReader.GetName(i), dbfReader.DbfRecord.Values[i].ToString() == "T" ? true : false);
                            }
                            else
                            {
                                json_object.Add(dbfReader.GetName(i), dbfReader.DbfRecord.Values[i].ToString());
                            }
                        }
                    }
                    rs.Add(json_object);
                }
                
                StreamWriter fs = new StreamWriter(new FileStream(String.Format("./exportData/export.json"), FileMode.Create));
                fs.WriteLine(rs.ToString());
                fs.Close();
                
            }

        }

    }
}