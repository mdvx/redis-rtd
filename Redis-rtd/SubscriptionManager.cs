using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using StackExchange.Redis;

namespace RedisRtd
{
    public class SubscriptionManager
    {
        public static readonly string UninitializedValue = "<?>";

        ConnectionMultiplexer redis;
        IDatabase db;
        ISubscriber sub;

        readonly Dictionary<string, SubInfo> _subByPath;
        readonly Dictionary<string, SubInfo> _subByRabbitPath;
        readonly Dictionary<int, SubInfo> _subByTopicId;
        readonly Dictionary<int, SubInfo> _dirtyMap;


        public SubscriptionManager()
        {
            redis = ConnectionMultiplexer.Connect("localhost");
            db = redis.GetDatabase();
            sub = redis.GetSubscriber();

            _subByRabbitPath = new Dictionary<string, SubInfo>();
            _subByPath = new Dictionary<string, SubInfo>();
            _subByTopicId = new Dictionary<int, SubInfo>();
            _dirtyMap = new Dictionary<int, SubInfo>();
        }

        public bool IsDirty {
            get {
                return _dirtyMap.Count > 0;
            }
        }

        public bool Subscribe(int topicId, string host, string key)
        {
            var subInfo = new SubInfo(topicId, key);
            _subByTopicId.Add(topicId, subInfo);
            _subByPath.Add(key, subInfo);

            sub.Subscribe(key, (channel, message) => {
                Console.WriteLine((string)message);
                Set(key, message);
            });

            return true;
        }
        //public bool Subscribe(int topicId, string host, string exchange, string routingKey, string field)
        //{
        //    var rabbitPath = FormatPath(host, exchange, routingKey);
        //    var rtdPath = FormatPath(host, exchange, routingKey, field);

        //    var alreadySubscribed = false;

        //    if (_subByRabbitPath.TryGetValue(rabbitPath, out SubInfo subInfo))
        //    {
        //        alreadySubscribed = true;
        //        subInfo.AddField(field);
        //    }
        //    else
        //    {
        //        subInfo = new SubInfo(topicId, rabbitPath);
        //        subInfo.AddField(field);
        //        _subByRabbitPath.Add(rabbitPath, subInfo);
        //    }

        //    SubInfo rtdSubInfo = new SubInfo(topicId, rtdPath);
        //    _subByTopicId.Add(topicId, rtdSubInfo);
        //    _subByPath.Add(rtdPath, rtdSubInfo);

        //    return alreadySubscribed;
        //}

        public void Unsubscribe(int topicId)
        {
            if (_subByTopicId.TryGetValue(topicId, out SubInfo subInfo))
            {
                //sub.Unsubscribe();

                _subByTopicId.Remove(topicId);
                _subByPath.Remove(subInfo.Path);
            }
        }

        public List<UpdatedValue> GetUpdatedValues()
        {
            var updated = new List<UpdatedValue>(_dirtyMap.Count);

            lock (_dirtyMap) { 
                foreach (var subInfo in _dirtyMap.Values)
                {
                    updated.Add(new UpdatedValue(subInfo.TopicId, subInfo.Value));
                }
                _dirtyMap.Clear();
            }

            return updated;
        }

        public bool Set(string path, object value)
        {
            if (_subByPath.TryGetValue(path, out SubInfo subInfo))
            {
                if (value != subInfo.Value)
                {
                    subInfo.Value = value;
                    lock (_dirtyMap)
                    {
                        _dirtyMap[subInfo.TopicId] = subInfo;
                    }
                    return true;
                }
            }
            return false;
        }

        [DebuggerStepThrough]
        public static string FormatPath(string host, string exchange, string routingKey)
        {
            return string.Format("{0}/{1}/{2}",
                                 host.ToUpperInvariant(),
                                 exchange.ToUpperInvariant(),
                                 routingKey.ToUpperInvariant());
        }
        [DebuggerStepThrough]
        public static string FormatPath(string host, string exchange, string routingKey, string field)
        {
            return string.Format("{0}/{1}", FormatPath(host, exchange, routingKey), field);
        }

        public class SubInfo
        {
            public int TopicId { get; private set; }
            public string Path { get; private set; }
            public HashSet<string> Fields { get; private set; }

            private object _value;

            public object Value
            {
                get { return _value; }
                set
                {
                    _value = value;
                }
            }

            public SubInfo(int topicId, string path)
            {
                TopicId = topicId;
                Path = path;
                Value = UninitializedValue;
                Fields = new HashSet<string>();
            }
            public void AddField(string field)
            {
                Fields.Add(field);
            }
        }
        public struct UpdatedValue
        {
            public int TopicId { get; private set; }
            public object Value { get; private set; }

            public UpdatedValue(int topicId, object value) : this()
            {
                TopicId = topicId;

                if (value is String)
                {
                   if (Decimal.TryParse(value.ToString(), out Decimal dec))
                        Value = dec;
                    else
                        Value = value;

                    if (dec > 1500_000_000_000 && dec < 1600_000_000_000)
                        Value = DateTimeOffset
                            .FromUnixTimeMilliseconds(Decimal.ToInt64(dec))
                            .DateTime
                            .ToLocalTime();
                }
                else
                {
                    Value = value;
                }
            }
        }
    }

}
