using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Replay;
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
        private static long? _globalCheckpoint;
        
        private readonly IStreamStore _streamStore;
        private readonly IReplayManager _replayManager;
        private readonly GetEventTypes _getEventTypes;
        private readonly EventSourcingOptions _options;
        private readonly ILogger<PersistentSubscriptionManager> _logger;
        
        private static readonly ConcurrentDictionary<Guid, (IStreamSubscription Subscription, int Failures, int CheckpointExpectedVersion)> Subscriptions
            = new ConcurrentDictionary<Guid, (IStreamSubscription Subscription, int Failures, int CheckpointExpectedVersion)>();

        public long? GlobalCheckpoint => _globalCheckpoint;

        public PersistentSubscriptionManager(IStreamStore streamStore, IReplayManager replayManager, GetEventTypes getEventTypes, IHostApplicationLifetime applicationLifetime,
            IOptions<EventSourcingOptions> options, ILogger<PersistentSubscriptionManager> logger)
        {
            _streamStore = streamStore;
            _replayManager = replayManager;
            _getEventTypes = getEventTypes;
            _options = options.Value;
            _logger = logger;
            
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

            var getGlobalCheckpointTask = Task.Run(async () => await GetGlobalCheckpoint());
            _globalCheckpoint = getGlobalCheckpointTask.Result;
        }
        
        public async Task SaveGlobalCheckpoint(long? checkpoint, CancellationToken cancellationToken = default)
        {
            _globalCheckpoint = checkpoint;

            if (checkpoint != default)
            {
                var checkpointStreamId = new StreamId(Constants.GlobalCheckpointId).GetCheckpointStreamId();
                var metadata = await _streamStore.GetStreamMetadata(checkpointStreamId, cancellationToken);
                if (metadata == null)
                    await _streamStore.SetStreamMetadata(checkpointStreamId, maxCount: 1, cancellationToken: cancellationToken);

                await _streamStore.AppendToStream(checkpointStreamId, ExpectedVersion.Any, new[]
                {
                    new NewStreamMessage(Guid.NewGuid(), Constants.CheckpointMessageType, checkpoint.ToString()),
                }, cancellationToken);   
            }
        }

        private async Task<long?> GetGlobalCheckpoint(CancellationToken cancellationToken = default)
        {
            var checkpointStreamId = new StreamId(Constants.GlobalCheckpointId).GetCheckpointStreamId();
            var metadata = await _streamStore.GetLastVersionOfStream<long?>(checkpointStreamId, cancellationToken);
            if (metadata.LastStreamVersion < 0)
                return null;

            return await metadata.GetDataFunc(cancellationToken);
        }

        public async Task SaveCheckpoint(int checkpoint, [CallerMemberName] string? checkpointName = default, CancellationToken cancellationToken = default)
        {
            var checkpointStreamId = new StreamId(checkpointName).GetCheckpointStreamId();
            var metadata = await _streamStore.GetStreamMetadata(checkpointStreamId, cancellationToken);
            if (metadata == null)
                await _streamStore.SetStreamMetadata(checkpointStreamId, maxCount: 1, cancellationToken: cancellationToken);

            var lastVersion = await _streamStore.GetLastVersionOfStream(checkpointStreamId, cancellationToken);
            await _streamStore.AppendToStream(checkpointStreamId, lastVersion + 1, new[]
            {
                new NewStreamMessage(Guid.NewGuid(), Constants.CheckpointMessageType, checkpoint.ToString())
            }, cancellationToken);
        }
        
        public async Task<Guid> CreateCategorySubscription(string categoryName, EventReceived eventReceived, CancellationToken cancellationToken = default)
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
                var catchUpCheckpoint = await _replayManager.ReplayProjectionFrom(categoryName, checkpoint, eventReceived, cancellationToken);
                if (catchUpCheckpoint != checkpoint)
                {
                    var appendResult = await _streamStore.AppendToStream(checkpointStreamId, checkpointVersion,
                        new[] {new NewStreamMessage(Guid.NewGuid(), "checkpoint", catchUpCheckpoint.ToString())},
                        cancellationToken);
                    
                    checkpointVersion = appendResult.CurrentVersion;
                    checkpoint = catchUpCheckpoint;
                }

                async Task StreamMessageReceived(IStreamSubscription subscription, StreamMessage message, CancellationToken token)
                {
                    _logger.LogInformation($"Processing event {message.Type} from subscription {subscription.Name}.");
                    var @event = await message.ToEvent(_getEventTypes(), token);
                    await eventReceived(@event, message.Position, message.StreamVersion, token);

                    var (_, failures, checkpointExpectedVersion) = Subscriptions[subscriptionId];
                    await _streamStore.AppendToStream(checkpointStreamId, checkpointExpectedVersion,
                        new[]
                        {
                            new NewStreamMessage(Guid.NewGuid(), Constants.CheckpointMessageType, subscription.LastVersion.ToString())
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

                var continueAfterVersion = checkpointVersion == ExpectedVersion.NoStream && checkpoint == StreamVersion.Start
                    ? (int?) null
                    : checkpoint;
                
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

        private static string BuildSubscriptionName(Guid subscriptionId, string descriptor)
            => $"{subscriptionId}:{descriptor}";

        private static string DeriveSubscriptionDescriptor(string subscriptionName)
            => subscriptionName.Substring(subscriptionName.IndexOf(":", StringComparison.Ordinal) + 1);
    }
}