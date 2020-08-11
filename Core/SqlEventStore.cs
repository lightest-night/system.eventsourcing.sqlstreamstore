using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Domain;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Persistence;
using LightestNight.System.ServiceResolution;
using LightestNight.System.Utilities.Extensions;
using SqlStreamStore;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public class SqlEventStore : IEventPersistence, IDisposable, IAsyncDisposable
    {
        private bool _disposed;
        
        private readonly IStreamStore _streamStore;
        private readonly ServiceFactory _serviceFactory;
        private readonly GetEventTypes _getEventTypes;

        public SqlEventStore(IStreamStore streamStore, ServiceFactory serviceFactory, GetEventTypes getEventTypes)
        {
            _streamStore = streamStore;
            _serviceFactory = serviceFactory;
            _getEventTypes = getEventTypes;
        }

        public async Task<TAggregate> GetById<TAggregate>(object id, CancellationToken cancellationToken = default) 
            where TAggregate : class, IEventSourceAggregate
        {
            var events = new List<EventSourceEvent>();
            var streamId = GenerateStreamId<TAggregate>(id);

            var page = await _streamStore.ReadStreamForwards(streamId, StreamVersion.Start, 200, cancellationToken: cancellationToken).ConfigureAwait(false);
            while (page.Messages.Any())
            {
                foreach (var message in page.Messages)
                    events.Add(await message.ToEvent(_getEventTypes(), cancellationToken).ConfigureAwait(false));

                page = await page.ReadNext(cancellationToken).ConfigureAwait(false);
            }

            var aggregate = _serviceFactory(typeof(TAggregate), events);
            if (aggregate == null)
                throw new NullReferenceException($"The aggregate of {typeof(TAggregate).Name} could not be built.");

            return (TAggregate) aggregate;
        }

        public async Task Save(IEventSourceAggregate aggregate, CancellationToken cancellationToken = default)
        {
            aggregate = aggregate.ThrowIfNull(nameof(aggregate));
            var events = aggregate.GetUncommittedEvents().ToArray();
            if (!events.Any())
                return;

            var streamId = GenerateStreamId(aggregate.GetType().Name, aggregate.Id);
            var originalVersion = aggregate.Version - events.Length;
            var expectedVersion = originalVersion == 0
                ? ExpectedVersion.NoStream
                : originalVersion - 1;
            
            var messagesToPersist = events.Select(evt => evt.ToMessageData()).ToArray();
            await _streamStore.AppendToStream(streamId, expectedVersion, messagesToPersist, cancellationToken);
            
            // We get all the way through the process, then we clear the uncommitted events. They are now processed, they are no longer *un*committed
            aggregate.ClearUncommittedEvents();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return new ValueTask();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
                _streamStore.Dispose();

            _disposed = true;
        }

        private static StreamId GenerateStreamId<T>(object id)
            => GenerateStreamId(typeof(T).Name, id);
        
        private static StreamId GenerateStreamId<TId>(string descriptor, TId id)
            => new StreamId($"{descriptor}-{id}");

        ~SqlEventStore()
        {
            Dispose(false);
        }
    }
}