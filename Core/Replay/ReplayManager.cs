using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Replay;
using Microsoft.Extensions.Logging;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Replay
{
    public class ReplayManager : IReplayManager
    {
        private readonly IStreamStoreFactory _streamStoreFactory;
        private readonly EventSourcingOptions _options;
        private readonly ILogger<ReplayManager> _logger;

        public ReplayManager(IStreamStoreFactory streamStoreFactory, EventSourcingOptions options, ILogger<ReplayManager> logger)
        {
            _streamStoreFactory = streamStoreFactory ?? throw new ArgumentNullException(nameof(streamStoreFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public async Task<long> ReplayProjectionFrom(long? fromCheckpoint, EventReceived eventReceived, [CallerMemberName]string? projectionName = default,
            CancellationToken cancellationToken = default)
        {
            var stopwatch = Stopwatch.StartNew();
            var streamStore = await _streamStoreFactory.GetStreamStore(cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            var page = await streamStore.ReadAllForwards(fromCheckpoint ?? Position.Start,
                _options.MaxReadStreamForward, false, cancellationToken).ConfigureAwait(false);
            while (page.Messages.Any())
            {
                foreach (var message in page.Messages)
                {
                    if (message.IsInSystemStream())
                    {
                        _logger.LogInformation($"Event {message.Type} is in a System stream therefore being skipped during replay.");
                        continue;
                    }
                    
                    var @event = await message.ToEvent(cancellationToken).ConfigureAwait(false);
                    await eventReceived(@event, message.Position, message.StreamVersion, cancellationToken).ConfigureAwait(false);
                }

                page = await page.ReadNext(cancellationToken).ConfigureAwait(false);
            }

            stopwatch.Stop();
            _logger.LogInformation($"{projectionName} replayed in {stopwatch.ElapsedMilliseconds}ms.");

            return page.NextPosition - 1;
        }

        public async Task<int> ReplayProjectionFrom(string streamId, int fromCheckpoint, EventReceived eventReceived, CancellationToken cancellationToken = default)
        {
            var streamStore = await _streamStoreFactory.GetStreamStore(cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            var streamVersion = await streamStore.GetLastVersionOfStream(streamId, cancellationToken).ConfigureAwait(false);
            if (streamVersion - fromCheckpoint <= _options.SubscriptionCheckpointDelta) 
                return fromCheckpoint;
            
            var stopwatch = Stopwatch.StartNew();
            var page = await streamStore.ReadStreamForwards(streamId, fromCheckpoint, _options.MaxReadStreamForward,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            while (page.Messages.Any())
            {
                foreach (var message in page.Messages)
                {
                    var @event = await message.ToEvent(cancellationToken).ConfigureAwait(false);
                    await eventReceived(@event, message.Position, message.StreamVersion, cancellationToken).ConfigureAwait(false);
                }

                page = await page.ReadNext(cancellationToken).ConfigureAwait(false);
            }

            stopwatch.Stop();
            _logger.LogInformation($"{streamId} caught up in {stopwatch.ElapsedMilliseconds}ms.");

            return page.LastStreamVersion;
        }
    }
}