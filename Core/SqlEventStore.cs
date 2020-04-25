using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Domain;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Persistence;
using LightestNight.System.EventSourcing.SqlStreamStore.Projections;
using LightestNight.System.ServiceResolution;
using LightestNight.System.Utilities;
using SqlStreamStore;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public class SqlEventStore : IEventPersistence
    {
        private readonly IStreamStore _streamStore;
        private readonly ServiceFactory _serviceFactory;
        private readonly GetEventTypes _getEventTypes;
        private readonly IEnumerable<IEventSourceProjection> _projections;

        private readonly EventWaitHandle _waitHandle = new AutoResetEvent(true);

        public SqlEventStore(IStreamStore streamStore, ServiceFactory serviceFactory, GetEventTypes getEventTypes, IEnumerable<IEventSourceProjection> projections)
        {
            _streamStore = streamStore;
            _serviceFactory = serviceFactory;
            _getEventTypes = getEventTypes;
            _projections = projections;
        }

        public async Task<T> GetById<T>(Guid id, CancellationToken cancellationToken = default) where T : class, IEventSourceAggregate
        {
            var events = new List<IEventSourceEvent>();
            var streamId = GenerateStreamId<T>(id);

            var page = await _streamStore.ReadStreamForwards(streamId, StreamVersion.Start, 200, cancellationToken: cancellationToken);
            while (page.Messages.Any())
            {
                var resolvedEvents = await Task.WhenAll(page.Messages.Select(DeserializeMessage));
                events.AddRange(resolvedEvents);

                page = await page.ReadNext(cancellationToken);
            }

            var aggregate = _serviceFactory(typeof(T), events);
            if (aggregate == null)
                throw new NullReferenceException($"The aggregate of {typeof(T).Name} could not be built.");

            return (T) aggregate;
        }

        public async Task Save(IEventSourceAggregate aggregate, CancellationToken cancellationToken = default)
        {
            var events = aggregate.GetUncommittedEvents().ToArray();
            if (!events.Any())
                return;

            var streamId = GenerateAggregateStreamId(aggregate);
            var originalVersion = aggregate.Version - events.Length;
            var expectedVersion = originalVersion == 0
                ? ExpectedVersion.NoStream
                : originalVersion - 1;

            _waitHandle.WaitOne();
            var messagesToPersist = events.Select(evt => ToMessageData(evt)).ToArray();
            var tasks = new List<Task>{_streamStore.AppendToStream(streamId, expectedVersion, messagesToPersist, cancellationToken)};
            tasks.AddRange(_projections.Select(projection => projection.ProcessEvents(streamId, messagesToPersist, cancellationToken)));
            await Task.WhenAll(tasks);
            _waitHandle.Set();
            
            // We get all the way through the process, then we clear the uncommitted events. They are now processed, they are no longer *un*committed
            aggregate.ClearUncommittedEvents();
        }

        private async Task<IEventSourceEvent> DeserializeMessage(StreamMessage message)
        {
            var messageJsonTask = message.GetJsonData();
            var typeName = message.Type;
            var version = 0;
            if (message.TryGetEventMetadata(Constants.VersionKey, out var versionMetaData))
                version = Convert.ToInt32(versionMetaData);

            var eventType = _getEventTypes().GetEventType(typeName, version);
            if (eventType == default)
                throw new InvalidOperationException($"Event Type could not be determined using type: {typeName} and version: {version}");

            var result = JsonSerializer.Deserialize(await messageJsonTask, eventType);
            return result as IEventSourceEvent ?? throw new InvalidOperationException($"Message could not be deserialized to an instance of {nameof(IEventSourceEvent)}.");
        }

        private static NewStreamMessage ToMessageData<TEvent>(TEvent @event, IDictionary<string, object>? headers = null)
        {
            var eventClrType = @event?.GetType() ?? throw new ArgumentNullException(nameof(@event));
            var typeName = EventTypeAttribute.GetEventTypeFrom(eventClrType);
            if (typeName == default)
                throw new ArgumentException("Event Type Name could not be determined", nameof(@event));

            headers ??= new Dictionary<string, object>();

            var version = Attributes.GetCustomAttributeValue<EventTypeAttribute, int>(eventClrType,
                eventTypeAttribute => eventTypeAttribute.Version);
            headers.Add(Constants.VersionKey, version);

            var data = JsonSerializer.Serialize(@event, eventClrType);
            var metadata = JsonSerializer.Serialize(headers);

            return new NewStreamMessage(Guid.NewGuid(), typeName, data, metadata);
        }

        private static StreamId GenerateAggregateStreamId(IEventSourceAggregate aggregate)
            => GenerateStreamId(aggregate.GetType().Name, aggregate.Id);
        
        private static StreamId GenerateStreamId<T>(Guid id)
            => GenerateStreamId(typeof(T).Name, id);
        
        private static StreamId GenerateStreamId(string descriptor, Guid id)
            => new StreamId($"{descriptor}-{id}");
    }
}