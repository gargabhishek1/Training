using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ReservationProcessor
{
    public class KafkaReservations 
    {

        private readonly IConfiguration _config;
        private readonly IProducer<Null, string> _producer;
        public KafkaReservations(IConfiguration config)
        {
            _config = config;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _config["Kafka:BootstrapServers"],
                ClientId = Dns.GetHostName()
            };

            _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        }

        public async Task WriteSuccessfullyProcessed(SuccessfulReservation reservation)
        {
            var json = JsonSerializer.Serialize(reservation);
            await _producer.ProduceAsync("reservations-success", new Message<Null, string> { Value = json });
        }

        public async Task WriteFailureToProcess(FailureReservation reservation)
        {
            var json = JsonSerializer.Serialize(reservation);
            await _producer.ProduceAsync("reservations-failure", new Message<Null, string> { Value = json });
        }
    }

    public class SuccessfulReservation
    {
        public string ReservationId { get; set; }
        public string For { get; set; }
        public List<Book> Books { get; set; }
        public virtual string Status { get; set; } = "Approved";
    }

    public class FailureReservation : SuccessfulReservation
    {
        public override string Status { get; set; } = "Failed";
        public string Message { get; set; }
    }
}
