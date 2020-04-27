using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Subscriptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlStreamStore;
using SqlStreamStore.Streams;
using SqlStreamStore.Subscriptions;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Subscriptions
{
    public class PersistentSubscriptionManager : IPersistentSubscriptionManager
    {
        private readonly IStreamStore _streamStore;
        private readonly ILogger<PersistentSubscriptionManager> _logger;
        private readonly GetEventTypes _getEventTypes;
        private readonly EventSourcingOptions _options;

        private static readonly ConcurrentDictionary<Guid, (IStreamSubscription Subscription, int Failures, int CheckpointExpectedVersion)> Subscriptions
            = new ConcurrentDictionary<Guid, (IStreamSubscription Subscription, int Failures, int CheckpointExpectedVersion)>();

        public PersistentSubscriptionManager(IStreamStore streamStore, ILogger<PersistentSubscriptionManager> logger, GetEventTypes getEventTypes, IHostApplicationLifetime applicationLifetime,
            IOptions<EventSourcingOptions> options)
        {
            _streamStore = streamStore;
            _logger = logger;
            _getEventTypes = getEventTypes;
            _options = options.Value;

            applicationLifetime.ApplicationStopping.Register(() =>
            {
                foreach (var subscriptionRecord in Subscriptions)
                {
                    var subscription = subscriptionRecord.Value.Subscription;

                    _logger.LogInformation($"Disposing persistent subscription: {subscription.Name}");
                    subscription.Dispose();
                }

                Subscriptions.Clear();
            });
        }

        public async Task<Guid> CreateCategorySubscription(string categoryName, Func<object, CancellationToken, Task> eventReceived, CancellationToken cancellationToken = default)
        {
            categoryName = categoryName.GetCategoryStreamId();
            
            var subscriptionId = Guid.NewGuid();
            var checkpointStreamId = new StreamId(categoryName).GetCheckpointStreamId();
            var checkpointStreamData = await _streamStore.GetLastVersionOfStream<int>(checkpointStreamId, cancellationToken);
            var checkpointVersion = checkpointStreamData.LastStreamVersion;
            var checkpoint = checkpointVersion < 0
                ? StreamVersion.Start
                : await checkpointStreamData.GetDataFunc(cancellationToken);

            IStreamSubscription? categorySubscription = null;
            try
            {
                var catchUpCheckpoint = await CatchSubscriptionUp(categoryName, checkpoint, eventReceived, cancellationToken);
                if (catchUpCheckpoint != checkpoint)
                {
                    await _streamStore.AppendToStream(checkpointStreamId, checkpointVersion,
                        new[] {new NewStreamMessage(Guid.NewGuid(), "checkpoint", catchUpCheckpoint.ToString())},
                        cancellationToken);

                    checkpoint = catchUpCheckpoint;
                }

                int? continueAfterVersion = checkpoint;
                if (continueAfterVersion < 0)
                    continueAfterVersion = null;

                async Task StreamMessageReceived(IStreamSubscription subscription, StreamMessage message, CancellationToken token)
                {
                    _logger.LogInformation($"Processing event {message.Type} from subscription {subscription.Name}.");
                    var @event = message.ToEvent(_getEventTypes(), token);
                    await eventReceived(@event, token);

                    var (_, failures, checkpointExpectedVersion) = Subscriptions[subscriptionId];
                    await _streamStore.AppendToStream(checkpointStreamId, checkpointExpectedVersion,
                        new[]
                        {
                            new NewStreamMessage(Guid.NewGuid(), "checkpoint", subscription.LastVersion.ToString())
                        }, token);

                    Subscriptions[subscriptionId] = (subscription, failures, checkpointExpectedVersion + 1);
                }

                void SubscriptionDropped(IStreamSubscription subscription, SubscriptionDroppedReason reason, Exception exception)
                {
                    var (sub, failures, version) = Subscriptions[subscriptionId];
                    failures += 1;
                        
                    if (failures > _options.SubscriptionRetryCount)
                    {
                        _logger.LogError(exception, $"Subscription {subscription.Name} dropped too many times, not reconnecting. Reason: {reason}");
                    }
                    else
                    {
                        _logger.LogInformation($"Subscription {subscription.Name} dropped, trying to reconnect. Reason: {reason}");
                        
                        sub.Dispose();
                        Subscriptions[subscriptionId] = (
                            _streamStore.SubscribeToStream(categoryName, subscription.LastVersion, StreamMessageReceived, SubscriptionDropped, name: subscription.Name),
                            failures,
                            version);
                    }
                }
                categorySubscription = _streamStore.SubscribeToStream(categoryName, continueAfterVersion,
                    StreamMessageReceived,
                    SubscriptionDropped,
                    name: BuildSubscriptionName(subscriptionId, categoryName));

                _logger.LogInformation($"{categorySubscription.Name} persistent subscription created.");
                if (!Subscriptions.TryAdd(subscriptionId, (categorySubscription, 0, checkpointVersion)))
                    throw new ApplicationException("Failed to add subscription");

                return subscriptionId;
            }
            catch
            {
                if (checkpointVersion == ExpectedVersion.NoStream)
                    await _streamStore.DeleteStream(checkpointStreamId, cancellationToken: cancellationToken);

                categorySubscription?.Dispose();

                throw;
            }
        }

        public async Task CloseSubscription(Guid subscriptionId, CancellationToken cancellationToken = new CancellationToken())
        {
            if (!Subscriptions.TryRemove(subscriptionId, out var subscriptionData))
                return;

            var subscription = subscriptionData.Subscription;
            var subscriptionName = subscription.Name;
            var subscriptionDescriptor = DeriveSubscriptionDescriptor(subscriptionName);
            await _streamStore.DeleteStream(new StreamId(subscriptionDescriptor).GetCheckpointStreamId(), cancellationToken: cancellationToken);
            subscription.Dispose();
            
            _logger.LogInformation($"{subscriptionName} subscription closed");
        }

        public async Task<int> CatchSubscriptionUp(string streamName, int checkpoint, Func<object, CancellationToken, Task> eventReceived, CancellationToken cancellationToken = default)
        {
            var streamVersion = await _streamStore.GetLastVersionOfStream(streamName, cancellationToken);
            if (streamVersion - checkpoint <= 50) 
                return checkpoint;
            
            var stopwatch = Stopwatch.StartNew();
            var page = await _streamStore.ReadStreamForwards(streamName, checkpoint, 200, cancellationToken: cancellationToken);
            while (page.Messages.Any())
            {
                var events = await Task.WhenAll(page.Messages.Select(message => message.ToEvent(_getEventTypes(), cancellationToken)));
                await Task.WhenAll(events.Select(@event => eventReceived(@event, cancellationToken)));

                page = await page.ReadNext(cancellationToken);
            }

            stopwatch.Stop();
            _logger.LogInformation($"{streamName} subscriber caught up in {stopwatch.ElapsedMilliseconds}ms.");

            return page.LastStreamVersion;
        }
        
        private static string BuildSubscriptionName(Guid subscriptionId, string descriptor)
            => $"{subscriptionId}:{descriptor}";

        private static string DeriveSubscriptionDescriptor(string subscriptionName)
            => subscriptionName.Substring(subscriptionName.IndexOf(":", StringComparison.Ordinal) + 1);
    }
}