using System;
using System.Collections.Generic;
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
                _options.MaxReadStreamForward, cancellationToken: cancellationToken).ConfigureAwait(false);

            var events = new Queue<EventSourceEvent>();
            while (page.Messages.Any())
            {
                foreach (var message in page.Messages.Where(msg => !msg.IsInSystemStream()))
                {
                    var @event = await message.ToEvent(cancellationToken).ConfigureAwait(false);
                    events.Enqueue(@event);
                }

                page = await page.ReadNext(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("Events to Replay: {eventCount}", events.Count);
            while (events.Count > 0)
            {
                var @event = events.Dequeue();
                await eventReceived(@event, cancellationToken).ConfigureAwait(false);
            }

            stopwatch.Stop();
            _logger.LogInformation("{projectionName} replayed in {ms}ms.", projectionName,
                stopwatch.ElapsedMilliseconds);

            return page.NextPosition - 1;
        }

        public async Task<int> ReplayProjectionFrom(string streamId, int fromCheckpoint, EventReceived eventReceived,
            CancellationToken cancellationToken = default)
        {
            var streamStore = await _streamStoreFactory.GetStreamStore(cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            var streamVersion =
                await streamStore.GetLastVersionOfStream(streamId, cancellationToken).ConfigureAwait(false);
            if (streamVersion - fromCheckpoint <= _options.SubscriptionCheckpointDelta)
                return fromCheckpoint;

            var stopwatch = Stopwatch.StartNew();
            var page = await streamStore.ReadStreamForwards(streamId, fromCheckpoint, _options.MaxReadStreamForward,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var events = new Queue<EventSourceEvent>();
            while (page.Messages.Any())
            {
                foreach (var message in page.Messages.Where(msg => !msg.IsInSystemStream()))
                {
                    var @event = await message.ToEvent(cancellationToken).ConfigureAwait(false);
                    events.Enqueue(@event);
                }

                page = await page.ReadNext(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("Events to Replay: {eventCount}", events.Count);
            while (events.Count > 0)
            {
                var @event = events.Dequeue();
                await eventReceived(@event, cancellationToken).ConfigureAwait(false);
            }

            stopwatch.Stop();
            _logger.LogInformation("{streamId} caught up in {ms}ms.", streamId, stopwatch.ElapsedMilliseconds);

            return page.LastStreamVersion;
        }
    }
}