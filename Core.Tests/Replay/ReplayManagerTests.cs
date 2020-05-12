using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Replay;
using LightestNight.System.EventSourcing.SqlStreamStore.Replay;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Replay
{
    public class ReplayManagerTests
    {
        private const string StreamId = "ReplayTests";
        private readonly Mock<IStreamStore> _streamStoreMock = new Mock<IStreamStore>();
        private readonly Mock<GetEventTypes> _getEventTypesMock = new Mock<GetEventTypes>();
        private readonly EventSourcingOptions _options = new EventSourcingOptions();
        
        private readonly IReplayManager _sut;

        protected ReplayManagerTests()
        {
            SetupReadStreamBackwards(StreamId);
            SetupReadStreamForwards(StreamId);
            
            _getEventTypesMock.Setup(getEventTypes => getEventTypes()).Returns(new[] {typeof(TestEvent)});
            _sut = new ReplayManager(_streamStoreMock.Object, Options.Create(_options), _getEventTypesMock.Object, Mock.Of<ILogger<ReplayManager>>());
        }

        public class StreamReplayTests : ReplayManagerTests
        {
            [Fact]
            public async Task Should_Get_Last_Version_When_Replaying()
            {
                // Act
                await _sut.ReplayProjectionFrom(StreamId, 0, (o, position, version, token) => Task.CompletedTask, CancellationToken.None);

                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                        It.Is<StreamId>(streamId => streamId.Value == StreamId),
                        StreamVersion.End,
                        It.IsAny<int>(),
                        It.IsAny<bool>(),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            }

            [Fact]
            public async Task Should_Read_The_Stream_From_Checkpoint()
            {
                // Arrange
                const int checkpoint = 10;
                SetupReadStreamBackwards(StreamId, 100);

                // Act
                await _sut.ReplayProjectionFrom(StreamId, checkpoint, (o, position, version, token) => Task.CompletedTask,
                    CancellationToken.None);

                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamForwards(
                        It.Is<StreamId>(streamId => streamId.Value == StreamId),
                        checkpoint,
                        It.IsAny<int>(),
                        It.IsAny<bool>(),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            }

            [Fact]
            public async Task Should_Fire_EventReceived_Function_When_Event_Found()
            {
                // Arrange
                const int checkpoint = 10;
                SetupReadStreamBackwards(StreamId, 100);

                var fired = false;

                Task EventReceived(object @event, long? position, int? version, CancellationToken token)
                {
                    fired = true;
                    return Task.CompletedTask;
                }

                // Act
                await _sut.ReplayProjectionFrom(StreamId, checkpoint, EventReceived, CancellationToken.None);

                // Assert
                fired.ShouldBeTrue();
            }
        }
        
        public class GlobalReplayTests : ReplayManagerTests
        {
            [Fact]
            public async Task Should_Read_The_Stream_From_Checkpoint()
            {
                // Arrange
                const long checkpoint = 10;
                SetupReadAllForwards();

                // Act
                await _sut.ReplayProjectionFrom(checkpoint, (o, position, version, token) => Task.CompletedTask,
                    cancellationToken: CancellationToken.None);

                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadAllForwards(
                    checkpoint,
                    It.IsAny<int>(),
                    It.IsAny<bool>(),
                    CancellationToken.None),
                    Times.Once);
            }

            [Fact]
            public async Task Should_Fire_EventReceived_Function_When_Event_Found()
            {
                // Arrange
                SetupReadAllForwards();
                const long checkpoint = 10;
                var fired = false;

                Task EventReceived(object @event, long? position, int? version, CancellationToken token)
                {
                    fired = true;
                    return Task.CompletedTask;
                }

                // Act
                await _sut.ReplayProjectionFrom(checkpoint, EventReceived, cancellationToken: CancellationToken.None);

                // Assert
                fired.ShouldBeTrue();
            }
        }
        
        private void SetupReadStreamBackwards(string streamId, int lastStreamVersion = ExpectedVersion.NoStream)
        {
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(
                    streamStore => streamStore.ReadStreamBackwards(streamId, StreamVersion.End, 1, true,
                        It.IsAny<CancellationToken>())
                )
                .ReturnsAsync(new ReadStreamPage(
                    streamId,
                    PageReadStatus.Success,
                    0,
                    0,
                    lastStreamVersion,
                    0,
                    ReadDirection.Backward,
                    true));
        }
        
        private void SetupReadStreamForwards(string streamId)
        {
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(
                    streamStore => streamStore.ReadStreamForwards(streamId, It.IsAny<int>(), It.IsAny<int>(),
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
                    new[]
                    {
                        new StreamMessage(streamId, Guid.NewGuid(), 0, 0, DateTime.UtcNow, nameof(TestEvent),
                            JsonSerializer.Serialize(new Dictionary<string, object> {{Constants.VersionKey, 0}}),
                            JsonSerializer.Serialize(new TestEvent {Id = Guid.NewGuid()}))
                    }
                ));
        }

        private void SetupReadAllForwards()
        {
            _streamStoreMock.As<IReadonlyStreamStore>().Setup(streamStore =>
                    streamStore.ReadAllForwards(It.IsAny<long>(), It.IsAny<int>(), It.IsAny<bool>(),
                        It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ReadAllPage(
                    Position.Start,
                    Position.Start + 1,
                    true,
                    ReadDirection.Forward,
                    (_, __) => Task.FromResult(new ReadAllPage(Position.Start, Position.Start + 1, true,
                        ReadDirection.Forward, null)),
                    new[]
                    {
                        new StreamMessage("Stream Message", Guid.NewGuid(), 0, 0, DateTime.UtcNow, nameof(TestEvent),
                            JsonSerializer.Serialize(new Dictionary<string, object> {{Constants.VersionKey, 0}}),
                            JsonSerializer.Serialize(new TestEvent {Id = Guid.NewGuid()}))
                    }
                ));
        }
    }
}