using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using RedisRtd;
using StackExchange.Redis;

namespace TestApp
{
    class Program : IRtdUpdateEvent
    {
        [STAThread]
        public static void Main(string[] args)
        {
            var me = new Program();
            IRtdUpdateEvent me2 = me;
            //me2.HeartbeatInterval = 15;  // is this seconds or milliseconds?
            me.Run();
        }

        IRtdServer _rtd;
        int _topic;
        bool consoleAppTest = false;   // false: test with excel, true: test with console app
        Random random = new Random();

        ISubscriber sub;

        void Run()
        {
            _rtd = new RedisRtdServer();
            _rtd.ServerStart(this);

            CancellationTokenSource cts = new CancellationTokenSource();

            for (int i = 0; i < 3; i++)
            {
                var json = "JSON_" + i;
                var raw = "RAW_" + i;

                Task.Run(() => PublishRedis(json, "FIELD", true, cts.Token));
                Task.Run(() => PublishRedis(raw, "FIELD", false, cts.Token));
            }

            // Start up a Windows message pump and spin forever.
            Dispatcher.Run();
        }
        void PublishRedis(string channel, string field, bool json, CancellationToken cts)
        {
            ConfigurationOptions options = new ConfigurationOptions
            {
                AbortOnConnectFail = true,
                EndPoints = { "localhost" }
            };
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(options);

            sub = redis.GetSubscriber();

            int r = 200;
            var padding = new String('x', r);

            int l = 0;
            while (!cts.IsCancellationRequested)
            {
                l++;

                var str = json ? $"{{ \"channel\": \"{channel}\", \"{field}\": {l}, \"len\": {r}, \"padding\": \"{padding}\"}}"   // choose between JSON
                               : $"{channel} => {field}: {l} {r} {padding}";         // and RAW

                sub.Publish(channel, str);

                if (l % 1000 == 0)
                {  
                    Console.WriteLine("sent " + str.Substring(0, Math.Min(75, str.Length)));

                    var d = random.NextDouble();
                    var e = random.Next(5);
                    r = (int)(d * Math.Pow(10, e));  // r should fall between 0 and 4*100,000

                    padding = new String('x', r+1);
                }
                Thread.Sleep(3);
            }

        }

        void Sub(string channel, string field)
        {
            Console.WriteLine($"Subscribing: topic={_topic}, exchange={channel}, field={field}");

            var a = new[] { "localhost", channel, field };
            Array crappyArray = a;

            bool newValues = false;
            _rtd.ConnectData(_topic++, ref crappyArray, ref newValues);
        }


        void IRtdUpdateEvent.UpdateNotify()
        {
            Console.WriteLine("UpdateNotified called ---------------------");

            int topicCount = 0;
            var values = _rtd.RefreshData(ref topicCount);

            for (int i = 0; i < topicCount; ++i)
            {
                Console.WriteLine(values.GetValue(0, i).ToString() + '\t' + values.GetValue(1, i).ToString());
            }
        }

        int IRtdUpdateEvent.HeartbeatInterval { get; set; }

        void IRtdUpdateEvent.Disconnect()
        {
            Console.WriteLine("Disconnect called.");
        }
    }
}
