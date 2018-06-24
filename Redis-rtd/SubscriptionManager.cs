using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace RedisRtd
{
    public class SubscriptionManager
    {
        public static readonly string UninitializedValue = "<?>";
        readonly Action _onDirty;

        readonly Dictionary<string, SubInfo> _subByTopicPath;
        readonly Dictionary<string, SubInfo> _subByRedisPath;
        readonly Dictionary<int, SubInfo> _subByTopicId;
        readonly Dictionary<int, SubInfo> _dirtyMap;

        public SubscriptionManager(Action onDirty)
        {
            _subByTopicId = new Dictionary<int, SubInfo>();
            _dirtyMap = new Dictionary<int, SubInfo>();
            _subByRedisPath = new Dictionary<string, SubInfo>();
            _subByTopicPath = new Dictionary<string, SubInfo>();
            _onDirty = onDirty;
        }

        public bool IsDirty {
            get {
                return _dirtyMap.Count > 0;
            }
        }

        public bool Subscribe(int topicId, string host, string channel, string field=null)
        {
            var redisPath = FormatPath(host, channel);
            var topicPath = FormatPath(host, channel, field);

            var alreadySubscribed = false;

            if (_subByRedisPath.TryGetValue(redisPath, out SubInfo subInfo))
            {
                alreadySubscribed = true;
                subInfo.AddField(field);
            }
            else
            {
                subInfo = new SubInfo(topicId, redisPath);
                subInfo.AddField(field);
                _subByRedisPath[redisPath] = subInfo;
            }

            SubInfo rtdSubInfo = new SubInfo(topicId, topicPath);
            _subByTopicId[topicId] = rtdSubInfo;
            _subByTopicPath[topicPath] = rtdSubInfo;

            return alreadySubscribed;
        }

        public void Unsubscribe(int topicId)
        {
            if (_subByTopicId.TryGetValue(topicId, out SubInfo subInfo))
            {
                //sub.Unsubscribe();

                _subByTopicId.Remove(topicId);
                _subByTopicPath.Remove(subInfo.Path);
            }
        }

        public object GetValue(int topicId)
        {
            return _subByTopicId[topicId]?.Value;
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
            if (_subByTopicPath.TryGetValue(path, out SubInfo subInfo))
            {
                if (value != subInfo.Value)
                {
                    subInfo.Value = value;
                    lock (_dirtyMap)
                    {
                        _dirtyMap[subInfo.TopicId] = subInfo;
                        _onDirty?.Invoke();
                    }
                    return true;
                }
            }
            return false;
        }
        [DebuggerStepThrough]
        public static string FormatPath(string host, string channel)
        {
            return string.Format($"{host.ToUpperInvariant()}/{channel}/");
        }
        [DebuggerStepThrough]
        public static string FormatPath(string host, string channel, string field)
        {
            return FormatPath(host,channel) + field;
        }

        public class SubInfo
        {
            public int TopicId { get; private set; }
            public string Path { get; private set; }
            public HashSet<string> Fields { get; private set; }

            public object Value { get; set; }

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
            public override string ToString()
            {
                return $"SubInfo topic={TopicId} path={Path} value={Value}";
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
