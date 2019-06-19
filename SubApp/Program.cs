using System;
using System.Diagnostics;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json;
using LoggerLib;
using NLog;
using System.Configuration;

namespace SubApp
{
    class Program
    {
       
        static void Main(string[] args)
        {

            string brokerList = ConfigurationManager.AppSettings["eventHubsNamespaceURL"];
            string connectionString = ConfigurationManager.AppSettings["eventHubsConnStr"];
            string topic = ConfigurationManager.AppSettings["eventHubName"];
            string caCertLocation = ConfigurationManager.AppSettings["caCertLocation"];
            string consumerGroup = ConfigurationManager.AppSettings["consumerGroup"];

            if (args.Length ==0)
            {
                Console.WriteLine("Specify the consumer number");
                return;
            }
            int consumerId = Int32.Parse(args[0]);  
           
            var conf = new ConsumerConfig
            {
                GroupId = consumerGroup,
               
                BootstrapServers = brokerList,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                SslCaLocation = caCertLocation,
                ApiVersionRequestTimeoutMs = 60000,
                BrokerVersionFallback = "1.0.0",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest                 
                
            };

            using (var c = new ConsumerBuilder<string, string>(conf).Build())
            {
                c.Subscribe("test");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };
                Stopwatch stopWatch = new Stopwatch();
                try
                {
                    stopWatch.Start();
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message with value -->'{cr.Value}' on partition {cr.TopicPartition.Partition.Value.ToString()} with offset --> {cr.TopicPartitionOffset.Offset.Value.ToString()}");
                            LogInfo(cr.Key, cr.Value, cr.TopicPartition.Partition.Value, cr.TopicPartitionOffset.Offset.Value, consumerId);
                            c.Commit(cr);
                            //Thread.Sleep(TimeSpan.FromMilliseconds(500));
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                            //stopWatch.Stop();
                        }
                    }

                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
                finally {
                    stopWatch.Stop();
                    TimeSpan ts = stopWatch.Elapsed;
                    Console.WriteLine("Took {0} milliseconds", ts.Milliseconds.ToString());
                    Console.ReadKey();
                }

            }
        }

        static void LogInfo(string messageKey, string message, int partitionNo, long partitionOffSet, int appInstance)
        {
            Logger logger = LogManager.GetLogger("databaseLogger");
            LogEventInfo logEventInfo = new LogEventInfo(LogLevel.Info, "databaseLogger", message);
            logEventInfo.Properties["MsgKey"] = messageKey;
            logEventInfo.Properties["AppName"] = "SubApp";
            logEventInfo.Properties["AppInstance"] = appInstance.ToString();
            logEventInfo.Properties["PartNo"] = partitionNo.ToString();
            logEventInfo.Properties["PartOffSet"] = partitionOffSet.ToString();
            logger.Info(logEventInfo);
        }
    }
}
