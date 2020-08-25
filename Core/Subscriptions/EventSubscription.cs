using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Checkpoints;
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
        private static IAllStreamSubscription? _subscription;
        private static int _failureCount;
        
        private readonly ILogger<EventSubscription> _logger;
        private readonly IStreamStore _streamStore;
        private readonly IEnumerable<IEventObserver> _eventObservers;
        private readonly GetGlobalCheckpoint _getGlobalCheckpoint;
        private readonly SetGlobalCheckpoint _setGlobalCheckpoint;
        
        public EventSubscription(ILogger<EventSubscription> logger, IStreamStore streamStore, IEnumerable<IEventObserver> eventObservers, SetGlobalCheckpoint setGlobalCheckpoint,
            GetGlobalCheckpoint getGlobalCheckpoint)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _streamStore = streamStore ?? throw new ArgumentNullException(nameof(streamStore));
            _eventObservers = eventObservers ?? throw new ArgumentNullException(nameof(eventObservers));
            _setGlobalCheckpoint = setGlobalCheckpoint ?? throw new ArgumentNullException(nameof(setGlobalCheckpoint));
            _getGlobalCheckpoint = getGlobalCheckpoint ?? throw new ArgumentNullException(nameof(getGlobalCheckpoint));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"{nameof(EventSubscription)} is starting...");
            stoppingToken.Register(() => _logger.LogInformation($"{nameof(EventSubscription)} is stopping..."));

            if (!_eventObservers.Any())
            {
                // There are no observers registered in the system, stop processing
                _logger.LogDebug("No event observers registered, nothing to do..");
                return;   
            }

            foreach (var eventObserver in _eventObservers)
            {
                eventObserver.PropertyChanged += async (sender, args) =>
                {
                    if (!(sender is IEventObserver observer)) 
                        return;
                    
                    if (observer.IsActive)
                        await SetSubscription(stoppingToken).ConfigureAwait(false);
                };
            }
            
            _logger.LogInformation($"There are {_eventObservers.Count()} observers registered.");
            await SetSubscription(stoppingToken).ConfigureAwait(false);
        }
        
        public override void Dispose()
        {
            _subscription?.Dispose();
            GC.SuppressFinalize(this);

            base.Dispose();
        }

        private async Task SetSubscription(CancellationToken cancellationToken = default)
        {
            if (_eventObservers.Any(eventObserver => !eventObserver.IsActive))
            {
                // There are observers that aren't active, so wait for them to become so
                // The subscription will be kicked off when all observers are active
                _logger.LogInformation("There are some observers that are inactive therefore the subscription will not be set up at this time.");
                return;
            }

            var checkpoint = await _getGlobalCheckpoint(cancellationToken).ConfigureAwait(false);
            
            _subscription = _streamStore.SubscribeToAll(checkpoint, StreamMessageReceived, SubscriptionDropped);
            _logger.LogInformation($"The {Constants.GlobalCheckpointId} subscription has been created.");
        }
        
        private async Task StreamMessageReceived(IAllStreamSubscription subscription, StreamMessage message, CancellationToken cancellationToken)
        {
            if (message.IsInSystemStream())
            {
                _logger.LogInformation($"Event {message.Type} is in a System stream therefore not being sent to observers.");
                return;
            }
            
            _logger.LogInformation($"Event {message.Type} received, sending to observers.");
            var eventSourceEvent = await message.ToEvent(cancellationToken).ConfigureAwait(false);
            await Task.WhenAll(_eventObservers.Select(observer => observer.EventReceived(eventSourceEvent, message.Position, message.StreamVersion, cancellationToken))).ConfigureAwait(false);
            
            await _setGlobalCheckpoint(subscription.LastPosition, cancellationToken).ConfigureAwait(false);
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
                _logger.LogError(exception, $"Event Subscription Dropped. Reason: {reason}. Failure #{_failureCount}. Attempting to reconnect...");
                _subscription = _streamStore.SubscribeToAll(subscription.LastPosition, StreamMessageReceived, SubscriptionDropped);
            }
        }
    }
}