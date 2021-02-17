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
                    Table table = Table.LoadTable(client, tableName);
                    Dictionary<string, AttributeValue> startKey = null;
                    MusicModel music = new MusicModel();
                    JArray jsonArray = new JArray();
                    
                    do{
                        ScanRequest request = new ScanRequest{
                            TableName = tableName,
                            ExclusiveStartKey = startKey,
                            ProjectionExpression = "id, Artist, songTitle, info, actv, idx",
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
                                music.id = item["id"].S;
                                music.Artist = item["Artist"].S;
                                music.songTitle = item["songTitle"].S;
                                Dictionary<string, string> dict = new Dictionary<string, string>();
                                dict.Add("album", item["info"].M["album"].S);
                                dict.Add("release", item["info"].M["release"].S);
                                music.info = dict;
                                music.idx = int.Parse(item["idx"].N);
                                music.actv = item["actv"].BOOL;
                                string jsonText = JsonConvert.SerializeObject(music, Formatting.None);
                                JObject json = JObject.Parse(jsonText);
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
            Table table = Table.LoadTable(client, (string)ar.GetValue("tableName", typeof(string)));
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
                        music.id = item["id"].S;
                        music.Artist = item["Artist"].S;
                        music.songTitle = item["songTitle"].S;
                        Dictionary<string, string> dict = new Dictionary<string, string>();
                        dict.Add("album", item["info"].M["album"].S);
                        dict.Add("release", item["info"].M["release"].S);
                        music.info = dict;
                        music.idx = int.Parse(item["idx"].N);
                        music.actv = item["actv"].BOOL;
                        string jsonText = JsonConvert.SerializeObject(music, Formatting.None);
                        JObject json = JObject.Parse(jsonText);
                        jsonArray.Add(json);
                        if(jsonArray.Count == 1000000){
                            Console.WriteLine("size : " + jsonArray.Count);
                            StreamWriter fs = new StreamWriter(new FileStream("./exportData/export.json", FileMode.Create));
                            fs.WriteLine(jsonArray.ToString());
                            fs.Close();
                            Console.WriteLine("saved export.json successfully!");
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

        public void import(string tableName){
            Stopwatch sw = new Stopwatch();
            sw.Start();
            AppSettingsReader ar = new AppSettingsReader();
            var credentials = new BasicAWSCredentials((string)ar.GetValue("accessKeyId", typeof(string)), (string)ar.GetValue("secretAccessKey", typeof(string)));
            var client = new AmazonDynamoDBClient(credentials, RegionEndpoint.APNortheast2);

            Console.WriteLine("import process executed => ");

            MusicModel music;
            Dictionary<string, List<WriteRequest>> reqItems = new Dictionary<string, List<WriteRequest>>();
            List<WriteRequest> writeReq = new List<WriteRequest>();
            Dictionary<string, AttributeValue> item;

            for(int fileNum = 0 ; fileNum < 10 ; fileNum++){
                JArray jsonArray = JArray.Parse(File.ReadAllText(String.Format("./exportData/export{0}.json", fileNum)));
                int count = 0, dummyNum = 0, i = 0;
                foreach(JObject obj in jsonArray){
                    item = new Dictionary<string, AttributeValue>();
                    music = obj.ToObject<MusicModel>();
                    item["dummy"] = new AttributeValue{ N = Convert.ToString(dummyNum++) };
                    item["id"] = new AttributeValue{ S = music.id };
                    item["Artist"] = new AttributeValue{ S = music.Artist };
                    item["songTitle"] = new AttributeValue{ S = music.songTitle };
                    Dictionary<string, AttributeValue> info = new Dictionary<string, AttributeValue>();
                    info["album"] = new AttributeValue { S = music.info["album"] };
                    info["release"] = new AttributeValue { S = music.info["release"] };
                    item["info"] = new AttributeValue{ M = info };
                    item["idx"] = new AttributeValue{ N = Convert.ToString(music.idx) };
                    item["actv"] = new AttributeValue{ BOOL = music.actv };
                    item["srchArtist"] = new AttributeValue{ S = music.Artist };
                    item["srchsongTitle"] = new AttributeValue{ S = music.songTitle };
                    item["srchidx"] = new AttributeValue{ N = Convert.ToString(music.idx) };
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

        public void convert(string convertOption){
            Console.WriteLine("reading export.json...");
            StreamReader sr = new StreamReader(new FileStream("./exportData/export7.json", FileMode.Open, FileAccess.Read));
            JArray jsonArray = JsonConvert.DeserializeObject<JArray>(sr.ReadToEnd());
            Console.WriteLine("read finished - item count : " + jsonArray.Count);
            StreamWriter sw = new StreamWriter(new FileStream("./exportData/export7.json", FileMode.OpenOrCreate, FileAccess.Write));
            switch(convertOption){

                // 데이터 추가
                case "add":
                    string command;
                    JObject inputData = new JObject();
                    Console.WriteLine("[System] 추가하고자 하는 항목의 데이터를 입력해주세요");
                    do {
                        Console.WriteLine("[System] 입력 예시 : [type] [Field Name] [Field Value]. 입력을 중단하려면 done을 입력하세요.");
                        Console.WriteLine(">> ");
                        command = Console.ReadLine();
                        string[] cmd = command.Split(" ");
                        if(cmd[0] == "s"){
                            inputData.Add(cmd[1], cmd[2]);
                        } else if(cmd[0] == "n"){
                            inputData.Add(cmd[1], Convert.ToInt32(cmd[2]));
                        } else if(cmd[0] == "b"){
                            inputData.Add(cmd[1], Convert.ToBoolean(cmd[2]));
                        } else if(cmd[0] == "m"){
                            string tempCommand;
                            JObject info = new JObject();
                            do{
                                Console.WriteLine("[System] {0}의 항목을 입력하세요 : [type] [Field Name] [Field Value]. 입력을 중단하려면 done을 입력하세요.", cmd[0]);
                                Console.WriteLine(">> ");
                                tempCommand = Console.ReadLine();
                                String[] tempCmd = tempCommand.Split(" ");
                                if(tempCmd[0] == "s"){
                                    info.Add(tempCmd[1], tempCmd[2]);
                                } else if(tempCmd[0] == "n"){
                                    info.Add(tempCmd[1], Convert.ToInt32(tempCmd[2]));
                                } else if(tempCmd[0] == "b"){
                                    info.Add(tempCmd[1], Convert.ToBoolean(tempCmd[2]));
                                }
                            } while(tempCommand != "done");
                            inputData.Add(cmd[1], info);
                        }
                    } while(command != "done");
                    Console.WriteLine(inputData);
                    jsonArray.Add(inputData);
                    sw.WriteLine(jsonArray.ToString());
                    sw.Close();
                    Console.WriteLine("added to export.json successfully!");
                    break;

                // 데이터 수정
                case "edit":
                    Console.WriteLine("[system] 수정하고자 하는 데이터의 id를 입력해주세요");
                    Console.Write(">> ");
                    command = Console.ReadLine();
                    JObject target = new JObject();
                    foreach(JObject json in jsonArray){
                        if(json.GetValue("id").ToString() == command){
                            target = json;
                        }
                    }
                    Console.WriteLine(target);

                    // 수정할 field와 그 값 입력받아서 target의 값 수정
                    break;





                // 데이터 삭제
                case "delete":
                    // edit과 마찬가지로 id로 찾고 해당 데이터 제외한 나머지 다시 저장
                    break;
            }
            return;
        }

        static void Main(string[] args)
        {
            DdbIntro db = new DdbIntro();
            string cmdLine = null;
            while(cmdLine != "exit"){
                Console.Write(">> ");
                cmdLine = Console.ReadLine();
                string[] cmd = cmdLine.Split(' ');
                switch(cmd[0]){
                    case "export":
                        if(cmd.Length == 2){
                            Console.WriteLine("export");
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
                        db.convert(cmd[1]);
                        GC.Collect();
                        break;
                }
            }
        }
    }
}
