using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Health.V1;
using XX.Framework.Collections.Extensions;
using XX.Framework.Utils;

namespace XX.Framework.Rpc
{
  public class GRPCClient
  {
    private static readonly ConcurrentDictionary<string, CachedItem> _cacheMap = new ConcurrentDictionary<string, CachedItem>();

    private static readonly object _lock = new object();

    private static Dictionary<string, IEnumerable<string>> _addressDict = null;

    private static int _executing = 0;

    private static TimeSpan _maintenanceStep = TimeSpan.FromMinutes(1);

    private static Timer _maintenanceTask = null;

    private static TimeSpan _timeout = TimeSpan.FromMinutes(1);

    public static int Count
    {
      get { return _cacheMap.Count; }
    }

    public static void Clear()
    {
      _cacheMap.Clear();
    }

    public static void DisposeChannels()
    {
      _cacheMap.ForEach(dict =>
      {
        dict.Value.Channels.ForEach(channel =>
        {
          channel.ShutdownAsync().Wait();
        });
      });
    }

    private static void DisposeChannelsByKey(string key)
    {
      Ensure.NotNullOrEmpty(key);
      if (_cacheMap.IsEmpty()) return;
      var item = _cacheMap.First(f => { return f.Key == key; });
      if (item.Key.IsEmpty()) return;
      if (item.Value.Channels.IsNotEmpty())
      {
        item.Value.Channels.ForEach(channel =>
        {
          channel.ShutdownAsync().Wait();
        });
      };
    }

    public static Channel GetWorkingChannel(string key)
    {
      Ensure.NotNullOrEmpty(key);
      CachedItem item = null;
      _cacheMap.TryGetValue(key, out item);
      Channel currentChosenChannel = null;
      if (_cacheMap.IsNotEmpty())
      {
        foreach (var channel in item.Channels.OrderBy(o => Guid.NewGuid()))
        {
          if (channel.State == ChannelState.Idle || channel.State == ChannelState.Ready)
          {
            currentChosenChannel = channel;
            break;
          }
        };
      }

      if (currentChosenChannel == null) InitWorkingChannelsByKey(key);
      return currentChosenChannel;
    }

    public static void Init()
    {
      string[] addressKeys = null;
      if (!string.IsNullOrWhiteSpace(ConfigurationManager.AppSettings["GrpcAddressSettingKeys"]))
      {
        addressKeys = ConfigurationManager.AppSettings["GrpcAddressSettingKeys"]
        .Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
      }
      _addressDict = new Dictionary<string, IEnumerable<string>>();
      Func<string, IEnumerable<string>> func = key =>
      {
        var addresses = ConfigurationManager.AppSettings[key]
              .Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
        return addresses;
      };

      addressKeys.ForEach(key =>
    {
      _addressDict.Add(key, func(key));
      InitWorkingChannelsByKey(key);
    });
    }

    public static IEnumerable<Channel> Remove(string key)
    {
      if (!String.IsNullOrEmpty(key))
      {
        CachedItem item;
        DisposeChannelsByKey(key);
        if (_cacheMap.TryRemove(key, out item))
        {
          return item.Channels;
        }
      }
      return null;
    }

    public static void Set(string key, IEnumerable<Channel> channels)
    {
      Ensure.NotNullOrEmpty(key);
      if (channels.IsEmpty())
      {
        Remove(key);
      }
      else
      {
        _cacheMap[key] = new CachedItem
        {
          Updated = DateTime.UtcNow,
          Channels = channels
        };

        StartMaintenance();
      }
    }

    private static bool CheckIfConnectionIsWorking(Channel serverChannel)
    {
      if (serverChannel != null)
      {
        try
        {
          var client = new Health.HealthClient(serverChannel);
          var response = client.Check(new HealthCheckRequest { Service = "HealthCheck" });
          return response.Status == HealthCheckResponse.Types.ServingStatus.Serving;
        }
        catch (Exception ex)
        {
          serverChannel.ShutdownAsync().Wait();
          return false;
        }
      }
      return false;
    }

    private static void ExecuteMaintenance(object state)
    {
      if (Interlocked.CompareExchange(ref _executing, 1, 0) != 0)
        return;
      try
      {
        if (_cacheMap.Count == 0)
        {
          StopMaintenance();
          if (_cacheMap.Count != 0)
            StartMaintenance();
        }
        else
        {
          CachedItem item;
          DateTime oldThreshold = DateTime.UtcNow - _timeout;
          var expiredItems = _cacheMap.Where(i => i.Value.Updated < oldThreshold).Select(i => i.Key);
          foreach (var key in expiredItems)
          {
            DisposeChannelsByKey(key);
            _cacheMap.TryRemove(key, out item);
            InitWorkingChannelsByKey(key);
          }
        }
      }
      finally
      {
        Interlocked.Exchange(ref _executing, 0);
      }
    }

    private static void InitWorkingChannelsByKey(string key)
    {
      Ensure.NotNullOrEmpty(key);
      if (!_addressDict.ContainsKey(key)) throw new ArgumentException("无法根据Key取到对应的Grpc配置");
      IList<Channel> channels = new List<Channel>();
      foreach (KeyValuePair<string, IEnumerable<string>> subAddress in _addressDict)
      {
        foreach (var address in subAddress.Value)
        {
          var channel = new Channel(address, ChannelCredentials.Insecure);
          if (CheckIfConnectionIsWorking(channel)) channels.Add(channel);
        }
      }

      Set(key, channels);
    }

    private static void StartMaintenance()
    {
      if (_maintenanceTask == null)
      {
        lock (_lock)
        {
          if (_maintenanceTask == null)
          {
            _maintenanceTask = new Timer(ExecuteMaintenance, null, _maintenanceStep, _maintenanceStep);
          }
        }
      }
    }

    private static void StopMaintenance()
    {
      lock (_lock)
      {
        if (_maintenanceTask != null)
          _maintenanceTask.Dispose();
        _maintenanceTask = null;
      }
    }

    private class CachedItem
    {
      public IEnumerable<Channel> Channels;
      public DateTime Updated;
    }
  }
}
