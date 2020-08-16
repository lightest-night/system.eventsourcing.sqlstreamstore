using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Persistence;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class SqlEventStoreTests
    {
        private readonly IStreamStore _streamStore = new InMemoryStreamStore();
        private readonly IEventPersistence _sut;

        public SqlEventStoreTests()
        {
            
            _sut = new SqlEventStore(_streamStore, Activator.CreateInstance);
        }

        [Fact]
        public async Task ShouldGetAggregateFromEventStream()
        {
            // Arrange
            var aggregateId = Guid.NewGuid();
            var events = new[] {new TestEvent(aggregateId)};
            await _streamStore.AppendToStream(_sut.GetStreamId(new TestAggregate(events)), ExpectedVersion.NoStream,
                events.Select(@event => @event.ToMessageData()).ToArray(), CancellationToken.None);

            // Act
            var aggregate = await _sut.GetById<TestAggregate>(aggregateId).ConfigureAwait(false);
            
            // Assert
            aggregate.Id.ShouldBe(aggregateId);
        }

        [Fact]
        public async Task ShouldAddNewAggregate()
        {
            // Arrange
            var aggregate = new TestAggregate();
            
            // Act
            await _sut.Save(aggregate);
            
            // Assert
            var streamId = _sut.GetStreamId(aggregate);
            var streams = await _streamStore.ListStreams(Pattern.StartsWith(streamId));
            streams.StreamIds.Length.ShouldBe(1);
            var events = (await _streamStore.ReadStreamForwards(streamId, StreamVersion.Start, 1)).Messages;
            (await Task.WhenAll(events.Select(@event => @event.ToEvent(CancellationToken.None))))
                .All(@event => @event.GetType() == typeof(TestEvent)).ShouldBeTrue();
        }

        [Fact]
        public async Task ShouldAddNewEventToExistingAggregate()
        {
            // Arrange
            var aggregate = new TestAggregate();
            await _sut.Save(aggregate);
            
            // Act
            aggregate = await _sut.GetById<TestAggregate>(aggregate.Id, CancellationToken.None);
            aggregate.SecondaryEvent();
            await _sut.Save(aggregate);
            
            // Assert
            var streamId = _sut.GetStreamId(aggregate);
            var events = (await _streamStore.ReadStreamForwards(streamId, StreamVersion.Start, 100)).Messages;
            events.Length.ShouldBe(2);
            (await Task.WhenAll(events.Select(@event => @event.ToEvent(CancellationToken.None))))
                .All(@event => @event.GetType() == typeof(TestEvent) || @event.GetType() == typeof(SecondaryTestEvent)).ShouldBeTrue();
        }

        [Fact]
        public void ShouldGetFullStreamId()
        {
            // Arrange
            var aggregate = new TestAggregate();
            
            // Act
            var result = _sut.GetStreamId(aggregate);
            
            // Assert
            result.ShouldContain(nameof(TestAggregate));
            result.ShouldContain(aggregate.Id.ToString());
        }
    }
}