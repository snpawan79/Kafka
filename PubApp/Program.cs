using System;
using System.Diagnostics;
using Confluent.Kafka;
using System.Threading.Tasks;
using System.Configuration;
using NLog;
using System.Collections.Generic;

namespace PubApp
{
    class Program
    {
        
        static async Task Main(string[] args)
        {

          
            string brokerList = ConfigurationManager.AppSettings["eventHubsNamespaceURL"];
            // connectionstring - primary or secondary key
            string password = ConfigurationManager.AppSettings["eventHubsConnStr"];
            // <youreventhubinstance>
            string topicName = ConfigurationManager.AppSettings["eventHubName"];
            // a location to a cache of ca certificates
            string caCertLocation = ConfigurationManager.AppSettings["caCertLocation"];
            //var config = new Dictionary<string, string> {
            //        { "bootstrap.servers", brokerList },
            //        { "security.protocol","SASL_SSL" },
            //        { "sasl.mechanism","PLAIN" },
            //        { "sasl.username", "$ConnectionString"},
            //        { "sasl.password", password },
            //        { "ssl.ca.location",caCertLocation },
            //        //{ "debug", "security,broker,protocol" } //Uncomment for librdkafka debugging information
            //};
            var config = new ProducerConfig
            {
                BootstrapServers = brokerList,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = password,
                SslCaLocation = caCertLocation,
            };
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                Stopwatch stopWatch = new Stopwatch();
                int ctr = 250;
                Random random = new Random();
                stopWatch.Start();
                string[] messageKeys = { "WO1", "WO2", "WO3", "WO4" };
               
                for (int i = 0; i < ctr; ++i)
                {
                    foreach(string messageKey in messageKeys)
                    {
                        var dr = await p.ProduceAsync("test", new Message<string, string> { Key = messageKey, Value = messageKey + " --> " + i.ToString() });
                        LogInfo(dr.Key, dr.Value, dr.TopicPartition.Partition.Value, dr.TopicPartitionOffset.Offset.Value, 0);
                    }             
                   
                }
                stopWatch.Stop();
                TimeSpan ts = stopWatch.Elapsed;
                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine("Took {0} milliseconds to write {1} records", ts.Milliseconds.ToString(), ctr.ToString());
                Console.ReadKey();
            }
            
        }
        static void LogInfo(string messageKey, string message,int partitionNo,long partitionOffSet,int appInstance)
        {
            Logger logger = LogManager.GetLogger("databaseLogger");
            LogEventInfo logEventInfo = new LogEventInfo(LogLevel.Info, "databaseLogger", message);
            logEventInfo.Properties["MsgKey"] = messageKey;
            logEventInfo.Properties["AppName"] = "PubApp";
            logEventInfo.Properties["AppInstance"] = appInstance.ToString();
            logEventInfo.Properties["PartNo"] = partitionNo.ToString();
            logEventInfo.Properties["PartOffSet"] = partitionOffSet.ToString();
            logger.Info(logEventInfo);
        }
    }
}
