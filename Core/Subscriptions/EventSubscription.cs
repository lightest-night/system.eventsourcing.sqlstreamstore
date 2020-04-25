using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SqlStreamStore;
using SqlStreamStore.Streams;
using SqlStreamStore.Subscriptions;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Subscriptions
{
    public class EventSubscription : BackgroundService
    {
        private readonly ILogger<EventSubscription> _logger;
        private readonly IStreamStore _streamStore;
        private readonly IEnumerable<IEventObserver> _eventObservers;
        private readonly GetEventTypes _getEventTypes;

        private IAllStreamSubscription? _subscription;
        private int _failureCount = 0;

        public EventSubscription(ILogger<EventSubscription> logger, IStreamStore streamStore, IEnumerable<IEventObserver> eventObservers, GetEventTypes eventTypes)
        {
            _logger = logger;
            _streamStore = streamStore;
            _eventObservers = eventObservers;
            _getEventTypes = eventTypes;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"{nameof(EventSubscription)} is starting...");
            stoppingToken.Register(() => _logger.LogInformation($"{nameof(EventSubscription)} is stopping..."));

            if (!_eventObservers.Any())
                // There are no observers registered in the system, stop processing
                return Task.CompletedTask;

            _subscription = _streamStore.SubscribeToAll(null, StreamMessageReceived, SubscriptionDropped);
            return Task.CompletedTask;
        }

        private async Task StreamMessageReceived(IAllStreamSubscription subscription, StreamMessage message, CancellationToken cancellationToken)
        {
            var eventSourceEvent = await message.ToEvent(_getEventTypes(), cancellationToken);
            await Task.WhenAll(_eventObservers.Select(observer => observer.EventReceived(eventSourceEvent, cancellationToken)));
        }

        private void SubscriptionDropped(IAllStreamSubscription subscription, SubscriptionDroppedReason reason, Exception exception)
        {
            _failureCount++;
            if (_failureCount >= 5)
            {
                _logger.LogError(exception, $"Event Subscription Dropped. Reason: {reason}");
            }
            else
            {
                _logger.LogInformation(
                    $"Event Subscription Dropped. Reason: {reason}. Failure #{_failureCount}. Attempting to reconnect...");
                _subscription = _streamStore.SubscribeToAll(subscription.LastPosition, StreamMessageReceived, SubscriptionDropped);
            }
        }
    }
}