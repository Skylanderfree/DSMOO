using System;
using System.Collections.Concurrent;
using DSMOOFramework.Config;
using DSMOOFramework.Logger;
using DSMOOFramework.Plugins;
using DSMOOServer.API.Events;
using DSMOOServer.API.Events.Args;

namespace DSMOOStability;

[Plugin(
    Name = "DSMOOStability",
    Description = "Reduces lag spikes by filtering packet floods",
    Author = "Skylanderfree",
    Version = "1.0.0",
    Repository = "local"
)]
public class DSMOOStability : Plugin<StabilityConfig>
{
    private readonly ILogger _logger;
    private readonly EventManager _eventManager;

    // per-client packet count
    private readonly ConcurrentDictionary<Guid, int> _packetCounter = new();

    private DateTime _lastReset = DateTime.UtcNow;
    private bool _highLoadMode;

    public DSMOOStability(ILogger logger, EventManager eventManager)
    {
        _logger = logger;
        _eventManager = eventManager;
    }

    public override void Initialize()
    {
        _logger.Info("[DSMOOStability] Loaded successfully");

        _eventManager.OnPacketReceived.Subscribe(OnPacket);
    }

    private void OnPacket(PacketReceivedEventArgs args)
    {
        if (!Config.Enabled)
            return;

        var id = args.Sender.Id;

        // increment per client
        _packetCounter.AddOrUpdate(id, 1, (_, v) => v + 1);

        // per-client flood protection
        if (_highLoadMode && _packetCounter[id] > Config.MaxPacketsPerClient)
        {
            args.Broadcast = false;
            return;
        }

        CheckGlobalLoad();
    }

    private void CheckGlobalLoad()
    {
        var now = DateTime.UtcNow;

        if ((now - _lastReset).TotalSeconds < 1)
            return;

        _lastReset = now;

        int total = 0;
        foreach (var v in _packetCounter.Values)
            total += v;

        _packetCounter.Clear();

        if (total > Config.HighLoadThreshold)
        {
            if (!_highLoadMode)
                _logger.Warn("[DSMOOStability] High load detected ? throttling enabled");

            _highLoadMode = true;
        }
        else if (total < Config.LowLoadThreshold)
        {
            if (_highLoadMode)
                _logger.Info("[DSMOOStability] Load normalized ? throttling disabled");

            _highLoadMode = false;
        }
    }
}

public class StabilityConfig : IConfig
{
    public bool Enabled { get; set; } = true;

    // tuning values
    public int MaxPacketsPerClient { get; set; } = 80;
    public int HighLoadThreshold { get; set; } = 500;
    public int LowLoadThreshold { get; set; } = 250;
}