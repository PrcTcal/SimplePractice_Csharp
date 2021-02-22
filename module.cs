using System;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Diagnostics;

namespace testModule{
    class modules {
        public static JObject addFunc(JObject json){
            if(Convert.ToInt32(json.GetValue("idx").ToString()) < 4000){
                json["upper"] = false;
            } else {
                json["upper"] = true;
            }

            if(Convert.ToInt32(json.GetValue("idx").ToString()) < 1000){
                json["info"]["final"] = new JObject();
                json["info"]["final"]["correct"] = true;
            }
            return json;
        }

        public static JObject editFunc(JObject json){
            if(json.GetValue("upper").ToString().ToLower() == "false"){
                json.Remove("upper");
                json["lower"] = true;
            }
            return json;
        }

        public static JObject deleteFunc(JObject json){
            if(json["upper"] != null) json.Remove("upper");
            if(json["lower"] != null) json.Remove("lower");
            return json;
        }
    }
}