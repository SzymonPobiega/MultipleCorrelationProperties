using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Persistence.Sql;

namespace MultipleCorrelationProperties
{
    class Program
    {
        static void Main(string[] args)
        {
            Start().GetAwaiter().GetResult();
        }

        static async Task Start()
        {
            var config = new EndpointConfiguration("MyEndpoint");
            config.UseTransport<MsmqTransport>();
            config.SendFailedMessagesTo("error");
            config.EnableInstallers();
            var persistence = config.UsePersistence<SqlPersistence>();
            persistence.ConnectionBuilder(() => new SqlConnection(@"Data Source=.\SQLEXPRESS;Initial Catalog=nservicebus;Integrated Security=True;"));
            persistence.SubscriptionSettings().DisableCache();
            persistence.SqlDialect<SqlDialect.MsSqlServer>();
            var instance = await Endpoint.Start(config);

            while (true)
            {
                Console.WriteLine("type '<customer> <order>' to send a new message.");
                var input = Console.ReadLine();
                var parts = input.Split(' ');
                var customer = parts[0];
                var order = parts[1];

                var message = new MyMessage()
                {
                    CustomerId = customer,
                    OrderId = order
                };

                await instance.SendLocal(message);
            }
            
        }
    }

    class MySaga : SqlSaga<MySagaData>, 
        IAmStartedByMessages<MyMessage>
    {
        protected override string CorrelationPropertyName => nameof(MySagaData.CorrelationId);

        protected override void ConfigureMapping(IMessagePropertyMapper mapper)
        {
            mapper.ConfigureMapping<MyMessage>(m => $"{m.CustomerId}-{m.OrderId}");
        }

        public Task Handle(MyMessage message, IMessageHandlerContext context)
        {
            if (!Data.Existing)
            {
                Console.WriteLine($"Creating a new saga instance for {message.CustomerId} and {message.OrderId}");
                Data.Existing = true;
                Data.OrderId = message.OrderId;
                Data.CustomerId = message.CustomerId;
                //Correlation property value assigned automatically
            }
            else
            {
                Console.WriteLine($"Using an existing instance for {message.CustomerId} and {message.OrderId}");
            }
            return Task.CompletedTask;
        }
    }

    class MyMessage : IMessage
    {
        public string OrderId { get; set; }
        public string CustomerId { get; set; }
    }

    class MySagaData : ContainSagaData
    {
        public bool Existing { get; set; }
        public string OrderId { get; set; }
        public string CustomerId { get; set; }
        public string CorrelationId { get; set; }
    }
}
