using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Subscriptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqlStreamStore;
using SqlStreamStore.Streams;
using SqlStreamStore.Subscriptions;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Subscriptions
{
    public class EventSubscription : BackgroundService
    {
        private static readonly StreamId AllStreamId = new StreamId("AllStream");
        private static readonly StreamId CheckpointStreamId = AllStreamId.GetCheckpointStreamId();
        private static IAllStreamSubscription? _subscription;
        private static int? _checkpointVersion;
        private static int _failureCount;
        
        private readonly ILogger<EventSubscription> _logger;
        private readonly IStreamStore _streamStore;
        private readonly IEnumerable<IEventObserver> _eventObservers;
        private readonly GetEventTypes _getEventTypes;
        private readonly IPersistentSubscriptionManager _persistentSubscriptionManager;

        public EventSubscription(ILogger<EventSubscription> logger, IStreamStore streamStore, IEnumerable<IEventObserver> eventObservers, GetEventTypes eventTypes,
            IPersistentSubscriptionManager persistentSubscriptionManager)
        {
            _logger = logger;
            _streamStore = streamStore;
            _eventObservers = eventObservers;
            _getEventTypes = eventTypes;
            _persistentSubscriptionManager = persistentSubscriptionManager;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"{nameof(EventSubscription)} is starting...");
            stoppingToken.Register(() => _logger.LogInformation($"{nameof(EventSubscription)} is stopping..."));

            if (!_eventObservers.Any())
                // There are no observers registered in the system, stop processing
                return;

            _logger.LogInformation($"There are {_eventObservers.Count()} observers registered.");
            foreach (var observer in _eventObservers)
                _logger.LogInformation($"{observer.GetType().Name} observer registered.");

            var checkpointData = await _streamStore.GetLastVersionOfStream<int>(CheckpointStreamId, stoppingToken);
            _checkpointVersion = checkpointData.LastStreamVersion;
            var checkpoint = _checkpointVersion < 0
                ? (int?) null
                : await checkpointData.GetDataFunc(stoppingToken);

            _subscription = _streamStore.SubscribeToAll(checkpoint, StreamMessageReceived, SubscriptionDropped);
        }

        public override void Dispose()
        {
            _subscription?.Dispose();
        }

        private async Task StreamMessageReceived(IAllStreamSubscription subscription, StreamMessage message, CancellationToken cancellationToken)
        {
            if (message.StreamId.StartsWith(Constants.SystemStreamPrefix))
            {
                _logger.LogInformation($"Event {message.Type} is in a System stream therefore not being sent to observers.");
                return;
            }

            _logger.LogInformation($"Event {message.Type} received, sending to observers.");
            var eventSourceEvent = await message.ToEvent(_getEventTypes(), cancellationToken);
            await Task.WhenAll(_eventObservers.Select(observer => observer.EventReceived(eventSourceEvent, cancellationToken)));
            
            await _persistentSubscriptionManager.SaveCheckpoint(Convert.ToInt32(subscription.LastPosition), CheckpointStreamId, cancellationToken);
        }

        private void SubscriptionDropped(IAllStreamSubscription subscription, SubscriptionDroppedReason reason, Exception exception)
        {
            _subscription?.Dispose();
            
            _failureCount++;
            if (_failureCount >= 5)
            {
                _logger.LogError(exception, $"Event Subscription Dropped. Reason: {reason}");
            }
            else
            {
                _logger.LogInformation($"Event Subscription Dropped. Reason: {reason}. Failure #{_failureCount}. Attempting to reconnect...");
                _subscription = _streamStore.SubscribeToAll(subscription.LastPosition, StreamMessageReceived, SubscriptionDropped);
            }
        }
    }
}