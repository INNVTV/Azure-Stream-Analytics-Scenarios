using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json;
using System.Text;

namespace EventProducer
{


    public class Worker : BackgroundService
    {
        // Create a SEND only key under "Shared acces policies" on the root hub namespace
        private string connectionString;
        private string eventHubName = "gates"; //<-- One of the many hubs created under the root namespace

        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            connectionString = Environment.GetEnvironmentVariable("ConnectionString");

            while (!stoppingToken.IsCancellationRequested)
            {
                // Create a producer client that you can use to send events to an event hub
                await using (var producerClient = new EventHubProducerClient(connectionString, eventHubName))
                {
                    // Create a batch of events
                    var options = new CreateBatchOptions();
                    //options.PartitionId = "3";//<--- Event Hubs will ensure this batch goes to the same partition (You are specifying the exact id)
                    //options.PartitionKey = "123";//<--- Event Hubs will ensure this batch goes to the same partition based on hash (EH will decide) RECOMMENDED BY MICROSOFT.
                    using EventDataBatch eventBatch = await producerClient.CreateBatchAsync(options);

                    #region Build telemetry payloads for this cycle and send to Event Hub

                    var worker1Id = Guid.NewGuid();
                    var worker2Id = Guid.NewGuid();
                    var worker3Id = Guid.NewGuid();
                    var worker4Id = Guid.NewGuid();
                    var worker5Id = Guid.NewGuid();
                    var worker6Id = Guid.NewGuid();

                    var gate1 = "1";
                    var gate2 = "2";
                    var gate3 = "3";
                    var gate4 = "4";
                    var gate5 = "5";

                    var telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };

                    #region Workers 1/2 Walk Through Gate 1

                    // Add events to the batch. An event is represented by a collection of bytes and metadata
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    
                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(100);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(250);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(550);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(100);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(250);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);

                    #endregion
                    Console.WriteLine("Workers 1 and 2 pass through gate 1");

                    await Task.Delay(250000);

                    #region Worker 1 Walks Through Gate 2

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(100);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(250);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(550);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(600);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);

                    #endregion
                    Console.WriteLine("Worker 1 passes through gate 2");

                    await Task.Delay(2000);

                    #region Worker 3 Walks Through Gate 1

                    await Task.Delay(100);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(250);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(150);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(560);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(220);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(450);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(700);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(330);
                    telemetryDataPoint = new
                    {
                        workerId = worker3Id,
                        gateId = gate1,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);

                    #endregion
                    Console.WriteLine("Worker 3 passes through gate 1");

                    await Task.Delay(20000);

                    #region Worker 2 Walks Through Gate 2

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(100);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(250);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(550);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(600);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate2,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);

                    #endregion
                    Console.WriteLine("Worker 2 passes through gate 2");
                    await Task.Delay(30000);

                    #region Worker 1 Walks Through Gate 3

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(200);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(100);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(250);
                    telemetryDataPoint = new
                    {
                        workerId = worker2Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(550);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(600);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    await Task.Delay(300);
                    telemetryDataPoint = new
                    {
                        workerId = worker1Id,
                        gateId = gate3,
                        timeEntered = DateTime.UtcNow
                    };
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(telemetryDataPoint))));

                    // Use the producer client to send the batch of events to the event hub
                    await producerClient.SendAsync(eventBatch);

                    #endregion
                    Console.WriteLine("Worker 1 passes through gate 3");

                    #endregion

                }

                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(300000, stoppingToken);
            }
        }
    }
}
