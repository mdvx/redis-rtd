using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;
using StackExchange.Redis;

namespace RedisRtd
{
    [
        Guid("0FD27211-C7C4-49D8-B6D9-0BF953DC88B0"),
        // This is the string that names RTD server.
        // Users will use it from Excel: =RTD("redis",, ....)
        ProgId("redis")
    ]
    public class RtdServer : IRtdServer
    {
        IRtdUpdateEvent _callback;

        //DispatcherTimer _timer;
        SubscriptionManager _subMgr;
        ISubscriber _redisSubscriber;
        bool _isExcelNotifiedOfUpdates = false;
        object _notifyLock = new object();

        private const string CLOCK = "CLOCK";
        private const string LAST_RTD = "LAST_RTD";

        public RtdServer ()
        {
            ConnectionMultiplexer redisConnection = ConnectionMultiplexer.Connect("localhost");
            //IDatabase redisDb = redisDb = redisConnection.GetDatabase();
            _redisSubscriber = redisConnection.GetSubscriber();
        }
        // Excel calls this. It's an entry point. It passes us a callback
        // structure which we save for later.
        int IRtdServer.ServerStart (IRtdUpdateEvent callback)
        {
            _callback = callback;
            _subMgr = new SubscriptionManager(() => {
                if (_callback != null)
                {
                    if (!_isExcelNotifiedOfUpdates)
                    {
                        lock (_notifyLock)
                            _isExcelNotifiedOfUpdates = true;

                        _callback.UpdateNotify();
                    }
                }
            });

            // We will throttle out updates so that Excel can keep up.
            // It is also important to invoke the Excel callback notify
            // function from the COM thread. System.Windows.Threading' 
            // DispatcherTimer will use COM thread's message pump.
            //DispatcherTimer dispatcherTimer = new DispatcherTimer();
            //_timer = dispatcherTimer;
            //_timer.Interval = TimeSpan.FromMilliseconds(95); // this needs to be very frequent
            //_timer.Tick += TimerElapsed;
            //_timer.Start();

            return 1;
        }

        // Excel calls this when it wants to shut down RTD server.
        void IRtdServer.ServerTerminate ()
        {
            _callback = null;
        }

        // Excel calls this when it wants to make a new topic subscription.
        // topicId becomes the key representing the subscription.
        // String array contains any aux data user provides to RTD macro.
        object IRtdServer.ConnectData (int topicId,
                                       ref Array strings,
                                       ref bool newValues)
        {
            if (strings.Length == 1)
            {
                string host = strings.GetValue(0).ToString().ToUpperInvariant();

                switch (host)
                {
                    case CLOCK:
                        lock (_subMgr)
                            _subMgr.Subscribe(topicId, null, CLOCK);

                        return DateTime.Now.ToLocalTime();

                    case LAST_RTD:
                        lock (_subMgr)
                            _subMgr.Subscribe(topicId, null, LAST_RTD);

                        return DateTime.Now.ToLocalTime();
                        //return SubscriptionManager.UninitializedValue;
                }
                return "ERROR: Expected: CLOCK or host, name, field";
            }
            else if (strings.Length >= 2)
            {
                newValues = true;

                // Crappy COM-style arrays...
                string host = strings.GetValue(0).ToString();
                string key = strings.GetValue(1).ToString();
                string field = strings.Length > 2 ? strings.GetValue(2).ToString() : "";

                return SubscribeRedis(topicId, host, key, field);
            }

            newValues = false;

            return "ERROR: Expected: CLOCK or host, key, field";
        }
        private object SubscribeRedis(int topicId, string host, string channel, string field)
        {
            lock (_subMgr)
            {
                if (_subMgr.Subscribe(topicId, host, channel, field))
                    return _subMgr.GetValue(topicId); // already subscribed 
            }
            _redisSubscriber.Subscribe(channel, (chan, message) => {
                try
                {
                    var str = message.ToString();

                    var rtdSubTopic = SubscriptionManager.FormatPath(host, chan);
                    _subMgr.Set(rtdSubTopic, str);

                    if (str.StartsWith("{"))
                    {
                        var jo = JsonConvert.DeserializeObject<Dictionary<String, String>>(str);

                        foreach (string field_in in jo.Keys)
                        {
                            var rtdTopicString = SubscriptionManager.FormatPath(host, channel, field_in);
                            _subMgr.Set(rtdTopicString, jo[field_in]);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            });

            return _subMgr.GetValue(topicId);
        }

        // Excel calls this when it wants to cancel subscription.
        void IRtdServer.DisconnectData (int topicId)
        {
            lock (_subMgr)
            {
                _subMgr.Unsubscribe(topicId);
            }
        }

        // Excel calls this every once in a while.
        int IRtdServer.Heartbeat ()
        {
            lock (_notifyLock)  // just in case it gets stuck
                _isExcelNotifiedOfUpdates = false;

            return 1;
        }

        // Excel calls this to get changed values. 
        Array IRtdServer.RefreshData (ref int topicCount)
        {
            var updates = GetUpdatedValues();
            topicCount = updates.Count;

            object[,] data = new object[2, topicCount];

            int i = 0;
            foreach (var info in updates)
            {
                data[0, i] = info.TopicId;
                data[1, i] = info.Value;

                i++;
            }

            lock (_notifyLock)
            {
                _isExcelNotifiedOfUpdates = false;
                return data;
            }
        }
        
        // Helper function which checks if new data is available and,
        // if so, notifies Excel about it.
        private void TimerElapsed (object sender, EventArgs e)
        {
            bool wasMarketDataUpdated;

            lock (_subMgr)
            {
                wasMarketDataUpdated = _subMgr.IsDirty;
            }

            if (wasMarketDataUpdated)
            {
                // Notify Excel that Market Data has been updated
                _subMgr.Set(LAST_RTD, DateTime.Now.ToLocalTime());
                _callback.UpdateNotify();
            }

            if (_subMgr.Set(CLOCK, DateTime.Now.ToLocalTime()))
                _callback.UpdateNotify();
        }

        List<SubscriptionManager.UpdatedValue> GetUpdatedValues ()
        {
            lock (_subMgr)
            {
                return _subMgr.GetUpdatedValues();
            }
        }
    }
}
