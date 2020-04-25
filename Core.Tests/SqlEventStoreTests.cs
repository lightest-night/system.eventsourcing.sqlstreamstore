using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Persistence;
using LightestNight.System.EventSourcing.SqlStreamStore.Projections;
using Moq;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class SqlEventStoreTests
    {
        private readonly Mock<IStreamStore> _streamStoreMock = new Mock<IStreamStore>();
        private readonly Mock<GetEventTypes> _getEventTypesMock = new Mock<GetEventTypes>();
        private readonly Mock<IEventSourceProjection> _projectionMock = new Mock<IEventSourceProjection>();
        private readonly IEventPersistence _sut;

        public SqlEventStoreTests()
        {
            _getEventTypesMock.Setup(getEventTypes => getEventTypes()).Returns(new[] {typeof(TestEvent)});
            _sut = new SqlEventStore(_streamStoreMock.Object, Activator.CreateInstance, _getEventTypesMock.Object, new[]{_projectionMock.Object});
        }

        [Fact]
        public async Task Should_Get_Aggregate_From_EventStream()
        {
            // Arrange
            var aggregateId = Guid.NewGuid();
            var events = new[] {new TestEvent {Id = aggregateId}};
            SetupReadStreamForwards(nameof(TestAggregate), events);

            // Act
            var aggregate = await _sut.GetById<TestAggregate>(aggregateId);
            
            // Assert
            aggregate.Id.ShouldBe(aggregateId);
        }

        [Fact]
        public async Task Should_Operate_NoOp_When_Saving_If_No_Events_Present()
        {
            // Arrange
            var aggregate = new TestAggregate(Enumerable.Empty<IEventSourceEvent>());
            
            // Act
            await _sut.Save(aggregate);
            
            // Assert
            _streamStoreMock.Verify(streamStoreMock => streamStoreMock.AppendToStream(
                It.IsAny<StreamId>(), It.IsAny<int>(), It.IsAny<NewStreamMessage[]>(), It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task Should_Add_New_Aggregate()
        {
            // Arrange
            var aggregate = new TestAggregate();
            
            // Act
            await _sut.Save(aggregate);
            
            // Assert
            _streamStoreMock.Verify(streamStoreMock => streamStoreMock.AppendToStream(
                It.Is<StreamId>(streamId => streamId.Value.Contains(aggregate.Id.ToString())),
                ExpectedVersion.NoStream,
                It.Is<NewStreamMessage[]>(messages => messages.Any(message => message.Type == nameof(TestEvent))),
                It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_Add_New_Event_To_Existing_Aggregate()
        {
            // Arrange
            var aggregate = new TestAggregate();
            await _sut.Save(aggregate);
            
            // Act
            aggregate.SecondaryEvent();
            await _sut.Save(aggregate);
            
            // Assert
            _streamStoreMock.Verify(streamStoreMock => streamStoreMock.AppendToStream(
                    It.Is<StreamId>(streamId => streamId.Value.Contains(aggregate.Id.ToString())),
                    It.Is<int>(version => version != ExpectedVersion.NoStream),
                    It.Is<NewStreamMessage[]>(messages => messages.Any(message => message.Type == nameof(TestEvent))),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task Should_Fire_Any_Projections_Registered()
        {
            // Arrange
            var aggregate = new TestAggregate();
            var projectionFired = false;
            _projectionMock.Setup(projection => projection.ProcessEvents(It.IsAny<string>(),
                    It.IsAny<NewStreamMessage[]>(), It.IsAny<CancellationToken>()))
                .Callback(() => projectionFired = true);
            
            // Act
            await _sut.Save(aggregate);
            
            // Assert
            projectionFired.ShouldBeTrue();
        }
        
        private void SetupReadStreamForwards(string streamId, IEnumerable<IEventSourceEvent> events)
        {
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(
                    streamStore => streamStore.ReadStreamForwards(It.Is<StreamId>(sId => sId.Value.Contains(streamId)), It.IsAny<int>(), It.IsAny<int>(),
                        It.IsAny<bool>(),
                        It.IsAny<CancellationToken>())
                )
                .ReturnsAsync(new ReadStreamPage(
                    streamId,
                    PageReadStatus.Success,
                    0,
                    0,
                    0,
                    0,
                    ReadDirection.Forward,
                    true,
                    (_, __) => Task.FromResult(new ReadStreamPage(streamId, PageReadStatus.Success, 0, 0, 0, 0,
                        ReadDirection.Forward, true)),
                    events.Select((e, idx) => 
                        new StreamMessage(
                            streamId, 
                            Guid.NewGuid(), 
                            idx, 
                            idx, 
                            DateTime.UtcNow, 
                            e.GetType().Name, 
                            JsonSerializer.Serialize(new Dictionary<string, object> {{Constants.VersionKey, 0}}),
                            JsonSerializer.Serialize(e))).ToArray()
                    ));
        }
    }
}