using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace ReservationProcessor
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _config;
        private readonly BooksLookupService _service;
        private readonly KafkaReservations _kafka;
        public Worker(ILogger<Worker> logger, IConfiguration config, BooksLookupService service, KafkaReservations kafka)
        {
            _logger = logger;
            _config = config;
            _service = service;
            _kafka = kafka;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            var config = new ConsumerConfig
            {
                BootstrapServers = _config["Kafka:BootstrapServers"],// "localhost:9092",
                GroupId = _config["Kafka:GroupId"],
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_config["Kafka:Topic"]);
            // reservation-reqeust => reservation-approved | reservation-denied
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumedResult = consumer.Consume(); // no async. This is a blocking call.
                var message = JsonSerializer.Deserialize<Reservation>(consumedResult.Message.Value);
                _logger.LogInformation($"Got a reservation  for {message.For} for the following books {message.Books.Select(b=>b.ToString() +",").ToList()}");
                var books = new List<Book>();
                var allGood = true;
                foreach(var bookId in message.Books)
                {
                    var response = await _service.CheckIfBookExists(bookId);
                    if(response.exists)
                    {
                        books.Add(response.book);
                    } else
                    {
                        allGood = false;
                        books.Add(new Book { Id = int.Parse(bookId), Title = "Non existent title", Author = "Non existent book" });
                    }
                }
                if(allGood)
                {
                    SuccessfulReservation res = new()
                    {
                        For = message.For,
                        Books = books,
                        ReservationId = message.ReservationId
                    };
                    _logger.LogInformation($"Successfully processed reservation {res.ReservationId}");
                    await _kafka.WriteSuccessfullyProcessed(res);
                } else
                {
                    FailureReservation res = new()
                    {
                        For = message.For,
                        Books = books,
                        ReservationId = message.ReservationId,
                        Message = "Could not process your reservation"
                    };
                    _logger.LogWarning($"Failed processing reservation {res.ReservationId}");
                    await _kafka.WriteFailureToProcess(res);
                }
            }

            consumer.Close();
        }
    }


    public class Reservation
    {
        public string ReservationId { get; set; }
        public string For { get; set; }
        public string[] Books { get; set; }
        public string Status { get; set; }
    }

}
