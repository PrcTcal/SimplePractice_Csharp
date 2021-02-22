using System;
using System.IO;
using System.Configuration;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
using testModule;

namespace DynamoDB_intro
{
    class MusicModel{

        public string id{
            get; set;
        }
        public string Artist{
            get; set;
        }
        public string songTitle{
            get; set;
        }
        public Dictionary<string, string> info{
            get; set;
        }
        public int idx{
            get; set;
        }
        public bool actv{
            get; set;
        }


        public override string ToString(){
            return "Item : {{\n\tid : " + id + "\n\tArtist : " + Artist + "\n\tsongTitle : " + songTitle + "\n\tinfo : " + info + 
            "\n\tidx : " + idx + "\n\tactv : " + actv + "\n}}";
        }

    }
    public partial class DdbIntro
    {

        public void export(string tableName){
            Stopwatch sw = new Stopwatch();
            sw.Start();
            AppSettingsReader ar = new AppSettingsReader();
            var credentials = new BasicAWSCredentials((string)ar.GetValue("accessKeyId", typeof(string)), (string)ar.GetValue("secretAccessKey", typeof(string)));
            var client = new AmazonDynamoDBClient(credentials, RegionEndpoint.APNortheast2);
            
            Console.WriteLine("export process executed => ");

            
            // 병렬 scan
            int totalSegments = 5;
            for(int k = 0 ; k < 2 ; k++){
                Parallel.For(k * totalSegments, (k + 1) * totalSegments, segment => {
                    //Console.WriteLine("segment : " + segment);
                    Dictionary<string, AttributeValue> startKey = null;
                    MusicModel music = new MusicModel();
                    JArray jsonArray = new JArray();
                    
                    do{
                        ScanRequest request = new ScanRequest{
                            TableName = tableName,
                            ExclusiveStartKey = startKey,
                            //ProjectionExpression = "id, Artist, songTitle, info, actv, idx",
                            FilterExpression = "dummy between :v_start and :v_end",
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue> {
                                {":v_start", new AttributeValue {
                                    N = Convert.ToString(segment * 500)
                                }},
                                {":v_end", new AttributeValue {
                                    N = Convert.ToString(((segment + 1) * 500) - 1)
                                }}
                            }
                        };
                        
                        ScanResult result = client.Scan(request);
                        List<Dictionary<string, AttributeValue>> items = result.Items;
                        foreach(Dictionary<string, AttributeValue> item in items){
                            try{
                                JObject json = getJObject(item);
                                jsonArray.Add(json);
                                if(jsonArray.Count == 100000){
                                    Console.WriteLine("size : " + jsonArray.Count);
                                    StreamWriter fs = new StreamWriter(new FileStream(String.Format("./exportData/export{0}.json", segment), FileMode.Create));
                                    fs.WriteLine(jsonArray.ToString());
                                    fs.Close();
                                    Console.WriteLine("saved export{0}.json successfully!", segment);
                                }
                            } catch(Exception e){
                                Console.WriteLine(e.StackTrace);
                            }
                        }
                        
                        Console.WriteLine(segment + " - size : " + jsonArray.Count);
                        startKey = result.LastEvaluatedKey;
                    } while(startKey != null && startKey.Count > 0);
                });
            }
            

            /*
            // 단일 scan
            Table table = Table.LoadTable(client, "test01-music2");
            Dictionary<string, AttributeValue> startKey = null;
            MusicModel music = new MusicModel();
            JArray jsonArray = new JArray();
            int fileNum = 0;
            
            do{
                ScanRequest request = new ScanRequest{
                    TableName = tableName,
                    ExclusiveStartKey = startKey,
                    ProjectionExpression = "id, Artist, songTitle, info, actv, idx"
                };
                ScanResult result = client.Scan(request);
                List<Dictionary<string, AttributeValue>> items = result.Items;
                
                foreach(Dictionary<string, AttributeValue> item in items){
                    try{
                        JObject json = getJObject(item);
                        jsonArray.Add(json);
                        if(jsonArray.Count == 1000000){
                            Console.WriteLine("size : " + jsonArray.Count);
                            StreamWriter fs = new StreamWriter(new FileStream($"./exportData/export{fileNum}.json", FileMode.Create));
                            fs.WriteLine(jsonArray.ToString());
                            fs.Close();
                            Console.WriteLine($"saved export{fileNum++}.json successfully!");
                            jsonArray = new JArray();
                        }
                    } catch(Exception e){
                        Console.WriteLine(e.StackTrace);
                    }
                }
                
                Console.WriteLine("size : " + jsonArray.Count);
                startKey = result.LastEvaluatedKey;
            } while(startKey != null && startKey.Count > 0);
            */

            sw.Stop();
            Console.WriteLine(sw.ElapsedMilliseconds.ToString() + "ms");
            Console.WriteLine("export process terminated");
            return;
        }

        public JObject getJObject(Dictionary<string, AttributeValue> obj){
            JObject result = new JObject();
            string[] excludeAttribute = new string[4]{"dummy", "srchArtist", "srchsongTitle", "srchidx"};
            var list = new List<string>();
            list.AddRange(excludeAttribute);
            foreach(var item in obj){
                if(!list.Contains(item.Key)){
                    if(item.Value.S != null){
                        result.Add(item.Key, item.Value.S);
                    } else if(item.Value.N != null){
                        result.Add(item.Key, Convert.ToInt32(item.Value.N));
                    } else if(item.Value.IsBOOLSet){
                        result.Add(item.Key, item.Value.BOOL);
                    } else if(item.Value.IsMSet){
                        Dictionary<string, AttributeValue> dic = new Dictionary<string, AttributeValue>();
                        foreach(var it in item.Value.M){
                            dic.Add(it.Key, it.Value);
                        }
                        result.Add(item.Key, getJObject(dic));
                    }
                }
                
            }
            return result;
        }

        public Dictionary<string, AttributeValue> getMap(JObject obj){
            Dictionary<string, AttributeValue> result = new Dictionary<string, AttributeValue>();
            
            foreach(var item in obj){
                //Console.WriteLine(item.Key + "\t" + item.Value.Type.ToString() + "\t" + item.Value.ToString());
                if(item.Value.Type.ToString() == "String"){
                    result[item.Key] = new AttributeValue { S = item.Value.ToString() };
                } else if(item.Value.Type.ToString() == "Integer"){
                    result[item.Key] = new AttributeValue { N = item.Value.ToString() };
                } else if(item.Value.Type.ToString() == "Boolean"){
                    result[item.Key] = new AttributeValue { BOOL = item.Value.ToString().ToLower() == "true" ? true : false };
                } else if(item.Value.Type.ToString() == "Object"){
                    result[item.Key] = new AttributeValue { M = getMap((JObject)item.Value) };
                }
            }
            
            return result;

        }

        public void import(string tableName){
            Stopwatch sw = new Stopwatch();
            sw.Start();
            AppSettingsReader ar = new AppSettingsReader();
            var credentials = new BasicAWSCredentials((string)ar.GetValue("accessKeyId", typeof(string)), (string)ar.GetValue("secretAccessKey", typeof(string)));
            var client = new AmazonDynamoDBClient(credentials, RegionEndpoint.APNortheast2);

            Console.WriteLine("import process executed => ");

            Dictionary<string, List<WriteRequest>> reqItems = new Dictionary<string, List<WriteRequest>>();
            List<WriteRequest> writeReq = new List<WriteRequest>();
            Dictionary<string, AttributeValue> item;

            for(int fileNum = 0 ; fileNum < 10 ; fileNum++){
                JArray jsonArray = JArray.Parse(File.ReadAllText(String.Format("./exportData/export{0}.json", fileNum)));
                int count = 0, dummyNum = 0, i = 0;
                foreach(JObject obj in jsonArray){
                    item = getMap(obj);
                    item["dummy"] = new AttributeValue{ N = Convert.ToString(dummyNum++) };
                    item["srchArtist"] = new AttributeValue{ S = obj.GetValue("Artist").ToString() };
                    item["srchsongTitle"] = new AttributeValue{ S = obj.GetValue("songTitle").ToString() };
                    item["srchidx"] = new AttributeValue{ N = Convert.ToString(obj.GetValue("idx").ToString()) };

                    writeReq.Add(new WriteRequest{
                        PutRequest = new PutRequest{ 
                            Item = item
                        }
                    });
                    if(dummyNum % 5000 == 0) dummyNum = 0;

                    if(i == 24){
                        i = 0;
                        reqItems[tableName] = writeReq;
                        BatchWriteItemRequest req = new BatchWriteItemRequest{ RequestItems = reqItems };
                        BatchWriteItemResult result;
                        do{
                            result = client.BatchWriteItem(req);
                            req.RequestItems = result.UnprocessedItems;
                            if(result.UnprocessedItems.Count > 0){
                                Console.WriteLine("Unprocessed Items remains - start retrying");
                            }
                        } while(result.UnprocessedItems.Count > 0);
                        count++;
                        Console.WriteLine("batch success => " + count);
                        writeReq = new List<WriteRequest>();
                    } else {
                        i++;
                    }
                    
                }
            }
            
            sw.Stop();
            Console.WriteLine(sw.ElapsedMilliseconds.ToString() + "ms");
            Console.WriteLine("import process terminated");
        }

        public JObject recurAdd(JObject param, string field){
            string command;
            do{
                Console.WriteLine("[System] {0}의 항목을 입력하세요 : [type] [Field Name] [Field Value]. 입력을 중단하려면 done을 입력하세요.", field);
                Console.Write(">> ");
                command = Console.ReadLine();
                string[] cmd = command.Split(" ");
                if(cmd[0] == "s"){
                    param.Add(cmd[1], cmd[2]);
                } else if(cmd[0] == "n"){
                    param.Add(cmd[1], Convert.ToInt32(cmd[2]));
                } else if(cmd[0] == "b"){
                    param.Add(cmd[1], Convert.ToBoolean(cmd[2]));
                } else if(cmd[0] == "m"){
                    param.Add(cmd[1], recurAdd(new JObject(), cmd[1]));
                }
            } while(command != "done");
            return param;
        }

        public void recurEdit(JObject json, string field){
            string command;
            do{
                Console.WriteLine("[System] {0}의 항목을 입력하세요 : [type] [Field Name] [Field Value]. 입력을 중단하려면 done을 입력하세요.", field);
                Console.Write(">> ");
                command = Console.ReadLine();
                string[] cmd = command.Split(" ");
                if(cmd[0] == "s"){
                    json[cmd[1]] = cmd[2];
                } else if(cmd[0] == "n"){
                    json[cmd[1]] = Convert.ToInt32(cmd[2]);
                } else if(cmd[0] == "b"){
                    json[cmd[1]] = Convert.ToBoolean(cmd[2]);
                } else if(cmd[0] == "m"){
                    recurEdit((JObject)json[cmd[1]], cmd[1]);
                }
            } while(command != "done");
        }

        public void convert(string convertOption, int fileNum){
            Console.WriteLine($"reading export{fileNum}.json...");
            StreamReader sr = new StreamReader(new FileStream($"./exportData/export{fileNum}.json", FileMode.Open, FileAccess.Read));
            JArray jsonArray = JsonConvert.DeserializeObject<JArray>(sr.ReadToEnd());
            sr.Close();
            Console.WriteLine("read finished - item count : " + jsonArray.Count);
            JArray result = new JArray();

            foreach(JObject obj in jsonArray){
                if(convertOption == "add") result.Add(modules.addFunc(obj));           // field 추가 모듈
                if(convertOption == "edit") result.Add(modules.editFunc(obj));        // field 수정 모듈
                if(convertOption == "delete") result.Add(modules.deleteFunc(obj));      // field 삭제 모듈
            }

            StreamWriter sw = new StreamWriter(new FileStream($"./exportData/export{fileNum}.json", FileMode.Create, FileAccess.Write));
            sw.WriteLine(jsonArray.ToString());
            sw.Close();
            Console.WriteLine($"added to export{fileNum}.json successfully!");
            return;
        }
        public void test(){
            AppSettingsReader ar = new AppSettingsReader();
            var credentials = new BasicAWSCredentials((string)ar.GetValue("accessKeyId", typeof(string)), (string)ar.GetValue("secretAccessKey", typeof(string)));
            var client = new AmazonDynamoDBClient(credentials, RegionEndpoint.APNortheast2);
            Table table = Table.LoadTable(client, "test01-music2");
            string[] fields;
            Console.WriteLine(table.Attributes);
            foreach(var item in table.Attributes){
                Console.WriteLine(item.AttributeName);
            }
        }
        static void Main(string[] args)
        {
            DdbIntro db = new DdbIntro();
            string cmdLine = null;
            while(cmdLine != "exit"){
                Console.WriteLine("=============================================<< commands >>=============================================");
                Console.WriteLine("export [Table name]                          : export data from Table");
                Console.WriteLine("import [Table name]                          : import data to Table");
                Console.WriteLine("convert [Convert Option]                     : convert data in export.json with converting option");
                Console.WriteLine("exit                                         : terminating CLI process");
                Console.WriteLine("========================================================================================================\n");
                Console.Write(">> ");
                cmdLine = Console.ReadLine();
                string[] cmd = cmdLine.Split(' ');
                switch(cmd[0]){
                    case "export":
                        if(cmd.Length == 2){
                            db.export(cmd[1]);
                            GC.Collect();
                        } else {
                            Console.WriteLine("incorrect command. use [export] [TableName]");
                        }
                        break;
                    case "import":
                        if(cmd.Length == 2){
                            db.import(cmd[1]);
                        } else {
                            Console.WriteLine("incorrect command. use [import] [TableName]");  
                        }    
                        break;
                    case "convert":
                        for(int i = 0 ; i < 10 ; i++){
                            db.convert(cmd[1], i);
                        }
                        GC.Collect();
                        break;
                    case "test":
                        db.test();
                        break;
                }
            }
        }
    }
}
