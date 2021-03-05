using System;
using System.IO;
using System.Configuration;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DBconnection{
    class connector{
        public static void connect(string option){
            AppSettingsReader ar = new AppSettingsReader();
            using(MySqlConnection conn = new MySqlConnection((string)ar.GetValue("mysqlConfig", typeof(string)))){
                if(option == "select"){
                    string select = "SELECT A.id, A.Artist, A.songTitle, IF(A.actv, 'true', 'false') as actv, A.idx, B.album, DATE_FORMAT(B.release_date, '%Y-%m-%d') as release_date FROM music A JOIN musicinfo B ON A.id = B.m_id LIMIT 5";

                    try{
                        conn.Open();
                        MySqlCommand cmd = new MySqlCommand(select, conn);
                        MySqlDataReader table = cmd.ExecuteReader();
                        
                        table.Read();
                        object[] objs = new object[7];
                        string[] names = new string[7];
                        for(int i = 0 ; i < 7 ; i++){
                            names[i] = table.GetName(i);
                        }
                        int quant = table.GetValues(objs);
                        Console.WriteLine(quant);
                        for(int i = 0 ; i < quant ; i++){
                            Console.WriteLine(names[i] + "\t" + objs[i] + "\t" + objs[i].GetType().ToString());
                        }
                        /*
                        JArray rs = new JArray();
                        while(table.Read()){
                            JObject obj = new JObject();
                            obj.Add("id", Convert.ToString(table["id"]));
                            obj.Add("Artist", Convert.ToString(table["Artist"]));
                            obj.Add("songTitle", Convert.ToString(table["songTitle"]));
                            obj.Add("actv", table["actv"].ToString() == "true" ? true : false);
                            obj.Add("idx", Convert.ToInt32(table["idx"]));
                            rs.Add(obj);
                            //Console.WriteLine("{0} {1} {2}", table["age"], table["name"], table["sex"]);
                        }
                        
                        Console.WriteLine("length : " + rs.Count);
                        StreamWriter fs = new StreamWriter(new FileStream(String.Format("./exportData/export.json"), FileMode.Create));
                        fs.WriteLine(rs.ToString());
                        fs.Close();
                        */
                        table.Close();
                        conn.Close();
                    } catch(Exception e){
                        Console.WriteLine(e.ToString());
                    }
                } else {
                    try{
                        conn.Open();
                        for(int fileNum = 0 ; fileNum < 10 ; fileNum++){
                            JArray jsonArray = JArray.Parse(File.ReadAllText(String.Format("./exportData/export{0}.json", fileNum)));
                            Console.WriteLine("inserting export{0}.json started - " + jsonArray.Count, fileNum);
                            int idx = 0;
                            
                            while(idx < jsonArray.Count){
                                string insertMusic = "INSERT INTO music (id, Artist, songTitle, actv, idx) VALUES ";
                                string insertMusicInfo = "INSERT INTO musicinfo (m_id, album, release_date) VALUES ";
                                insertMusic += String.Format("('{0}', '{1}', '{2}', {3}, {4})", jsonArray[idx]["id"], jsonArray[idx]["Artist"].ToString().Replace("'", "").Trim(), jsonArray[idx]["songTitle"].ToString().Replace("'", "").Trim(), jsonArray[idx]["actv"], jsonArray[idx]["idx"]);
                                insertMusicInfo += String.Format("('{0}', '{1}', '{2}')", jsonArray[idx]["id"], jsonArray[idx]["info"]["album"].ToString().Replace("'", "").Trim(), jsonArray[idx]["info"]["release"]);
                                idx++;
                                for(int i = 0 ; i < 999 ; i++){
                                    insertMusic += String.Format(", ('{0}', '{1}', '{2}', {3}, {4})", jsonArray[idx]["id"], jsonArray[idx]["Artist"].ToString().Replace("'", "").Trim(), jsonArray[idx]["songTitle"].ToString().Replace("'", "").Trim(), jsonArray[idx]["actv"], jsonArray[idx]["idx"]);
                                    insertMusicInfo += String.Format(", ('{0}', '{1}', '{2}')", jsonArray[idx]["id"], jsonArray[idx]["info"]["album"].ToString().Replace("'", "").Trim(), jsonArray[idx]["info"]["release"]);
                                    idx++;
                                }
                                Console.WriteLine("cur : " + (idx / 1000) + "%");
                                MySqlCommand cmd1 = new MySqlCommand(insertMusic, conn);
                                MySqlCommand cmd2 = new MySqlCommand(insertMusicInfo, conn);
                                cmd1.ExecuteNonQuery();
                                cmd2.ExecuteNonQuery();
                            }    
                        }
                        conn.Close();
                    } catch(Exception e){
                        Console.WriteLine(e.ToString());
                    }
                    
                }
                
            }
        }

        
    }
}