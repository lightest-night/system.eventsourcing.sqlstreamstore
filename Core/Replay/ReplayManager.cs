using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Replay;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlStreamStore;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Replay
{
    public class ReplayManager : IReplayManager
    {
        private readonly IStreamStore _streamStore;
        private readonly EventSourcingOptions _options;
        private readonly GetEventTypes _getEventTypes;
        private readonly ILogger<ReplayManager> _logger;

        public ReplayManager(IStreamStore streamStore, IOptions<EventSourcingOptions> options, GetEventTypes getEventTypes, ILogger<ReplayManager> logger)
        {
            _streamStore = streamStore;
            _getEventTypes = getEventTypes;
            _logger = logger;
            _options = options.Value;
        }

        public async Task<int> ReplayProjectionFrom(string streamId, int fromCheckpoint, Func<object, CancellationToken, Task> eventReceived, CancellationToken cancellationToken = default)
        {
            var streamVersion = await _streamStore.GetLastVersionOfStream(streamId, cancellationToken);
            if (streamVersion - fromCheckpoint <= _options.SubscriptionCheckpointDelta) 
                return fromCheckpoint;
            
            var stopwatch = Stopwatch.StartNew();
            var page = await _streamStore.ReadStreamForwards(streamId, fromCheckpoint, _options.MaxReadStreamForward, cancellationToken: cancellationToken);
            while (page.Messages.Any())
            {
                var events = await Task.WhenAll(page.Messages.Select(message => message.ToEvent(_getEventTypes(), cancellationToken)));
                await Task.WhenAll(events.Select(@event => eventReceived(@event, cancellationToken)));

                page = await page.ReadNext(cancellationToken);
            }

            stopwatch.Stop();
            _logger.LogInformation($"{streamId} caught up in {stopwatch.ElapsedMilliseconds}ms.");

            return page.LastStreamVersion;
        }
    }
}