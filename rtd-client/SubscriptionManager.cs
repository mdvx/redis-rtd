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
            var topicPath = FormatPath(host, channel, field);
            var alreadySubscribed = false;
            var redisPath = FormatPath(host, channel);

            if (_subByTopicId.TryGetValue(topicId, out SubInfo subInfo))
            {
                alreadySubscribed = true;
                subInfo.AddField(field);
            }
            else
            {
                subInfo = new SubInfo(topicId, redisPath);
                subInfo.AddField(field);
                _subByTopicId[topicId] = subInfo;
            }

            _subByRedisPath[redisPath] = subInfo;
            _subByTopicPath[topicPath] = subInfo;

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
            if (_subByTopicId.TryGetValue(topicId, out SubInfo sub))
                return ToExcelValue(sub.Value);

            return UninitializedValue;
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

        public void Set(int topicId, object value)
        {
            if (_subByTopicId.TryGetValue(topicId, out SubInfo subInfo))
            {
                subInfo.Value = value;
                lock (_dirtyMap)
                {
                    _dirtyMap[topicId] = subInfo;
                    _onDirty?.Invoke();
                }
            }
        }

        public bool Set(string path, object value)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentException("path is empty");

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
        public static string FormatPath(string host, string channel, string field=null)
        {
            return string.Format("{0}/{1}/{2}", host?.ToUpperInvariant(), channel, field);
        }

        public class SubInfo
        {
            public int TopicId { get; private set; }
            public string Path { get; private set; }
            public HashSet<string> Fields { get; private set; }

            public object Value { get; set; }

            public SubInfo(int topicId, string path, object value)
            {
                TopicId = topicId;
                Path = path;
                Value = value;
                Fields = new HashSet<string>();
            }
            public SubInfo(int topicId, string path) : this(topicId, path, UninitializedValue)
            {
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
                Value = ToExcelValue(value);
            }
        }

        private static object ToExcelValue(object val)
        {
            object result;
            if (val is String)
            {
                if (Decimal.TryParse(val.ToString(), out Decimal dec))
                    result = dec;
                else
                    result = val;

                if (dec > 1500_000_000_000 && dec < 1600_000_000_000)
                    result = DateTimeOffset
                        .FromUnixTimeMilliseconds(Decimal.ToInt64(dec))
                        .DateTime
                        .ToLocalTime();
            }
            else
            {
                result = val;
            }
            return result;
        }
    }
}
