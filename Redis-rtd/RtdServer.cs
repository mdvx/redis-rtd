using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Threading;

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
        DispatcherTimer _timer;
        readonly SubscriptionManager _subMgr;
        //Dictionary<string, IConnection> _connections = new Dictionary<string, IConnection>();

        private const string CLOCK = "CLOCK";
        private const string LAST_RTD = "LAST_RTD";

        public RtdServer ()
        {
            _subMgr = new SubscriptionManager();
        }
        // Excel calls this. It's an entry point. It passes us a callback
        // structure which we save for later.
        int IRtdServer.ServerStart (IRtdUpdateEvent callback)
        {
            _callback = callback;

            // We will throttle out updates so that Excel can keep up.
            // It is also important to invoke the Excel callback notify
            // function from the COM thread. System.Windows.Threading' 
            // DispatcherTimer will use COM thread's message pump.
            DispatcherTimer dispatcherTimer = new DispatcherTimer();
            _timer = dispatcherTimer;
            _timer.Interval = TimeSpan.FromMilliseconds(95); // this needs to be very frequent
            _timer.Tick += TimerElapsed;
            _timer.Start();

            return 1;
        }

        // Excel calls this when it wants to shut down RTD server.
        void IRtdServer.ServerTerminate ()
        {
            if (_timer != null)
            {
                _timer.Stop();
                _timer = null;
            }

            lock(_subMgr)
            {
                _callback.UpdateNotify();
            }
            //Thread.Sleep(2000);
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
                return "ERROR: Expected: CLOCK or host, exchange, routingKey, field";
            }
            else if (strings.Length >= 2)
            {
                newValues = true;

                // Crappy COM-style arrays...
                string host = strings.GetValue(0).ToString();
                string key = strings.GetValue(1).ToString();

                CancellationTokenSource cts = new CancellationTokenSource();
                Task.Run(() => Subscribe(topicId, host, key, cts.Token));

                return SubscriptionManager.UninitializedValue;
            }

            newValues = false;

            return "ERROR: Expected: CLOCK or host, exchange, routingKey, field";
        }

        private void Subscribe(int topicId, string host, string key, CancellationToken cts)
        {
            try
            {
                lock (_subMgr)
                {
                    if (_subMgr.Subscribe(topicId, host, key))
                        return; // already subscribed 
                }

            }
            catch(Exception e)
            {
        //        //ESLog.Error("SubscribeRabbit", e);
            }
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
            return 1;
        }

        // Excel calls this to get changed values. 
        Array IRtdServer.RefreshData (ref int topicCount)
        {
            var updates = GetUpdatedValues();
            topicCount = updates.Count;

            object[,] data = new object[2, topicCount];

            for (int i = 0; i < topicCount; ++i)
            {
                SubscriptionManager.UpdatedValue info = updates[i];

                data[0, i] = info.TopicId;
                data[1, i] = info.Value;
            }

            return data;
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
