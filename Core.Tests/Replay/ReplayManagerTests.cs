using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Replay;
using LightestNight.System.EventSourcing.SqlStreamStore.Replay;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Replay
{
    public abstract class ReplayManagerTestsFixture
    {
        protected const string StreamId = "ReplayTests";

        protected Mock<IStreamStore> StreamStoreMock { get; }
        private EventSourcingOptions EventSourcingOptions { get; }

        protected IReplayManager Sut { get; }

        protected ReplayManagerTestsFixture()
        {
            EventCollection.AddAssemblyTypes(Assembly.GetExecutingAssembly());
            StreamStoreMock = new Mock<IStreamStore>();
            EventSourcingOptions = new EventSourcingOptions();
            
            SetupReadStreamBackwards(StreamId);
            SetupReadStreamForwards(StreamId);

            var streamStoreFactoryMock = new Mock<IStreamStoreFactory>();
            streamStoreFactoryMock
                .Setup(streamStoreFactory => streamStoreFactory.GetStreamStore(3, CancellationToken.None))
                .ReturnsAsync(StreamStoreMock.Object);
            Sut = new ReplayManager(streamStoreFactoryMock.Object, EventSourcingOptions, Mock.Of<ILogger<ReplayManager>>());
        }

        protected void SetupReadStreamBackwards(string streamId, int lastStreamVersion = ExpectedVersion.NoStream)
        {
            StreamStoreMock.As<IReadonlyStreamStore>()
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
            StreamStoreMock.As<IReadonlyStreamStore>()
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
                            JsonSerializer.Serialize(new TestEvent(Guid.NewGuid())))
                    }
                ));
        }

        protected void SetupReadAllForwards()
        {
            StreamStoreMock.As<IReadonlyStreamStore>().Setup(streamStore =>
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
                            JsonSerializer.Serialize(new TestEvent(Guid.NewGuid())))
                    }
                ));
        }
    }

    public class StreamReplayTests : ReplayManagerTestsFixture
    {
        [Fact]
        public async Task ShouldGetLastVersionWhenReplaying()
        {
            // Act
            await Sut.ReplayProjectionFrom(StreamId, 0, (o, token) => Task.CompletedTask,
                CancellationToken.None).ConfigureAwait(false);

            // Assert
            StreamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                    It.Is<StreamId>(streamId => streamId.Value == ReplayManagerTestsFixture.StreamId),
                    StreamVersion.End,
                    It.IsAny<int>(),
                    It.IsAny<bool>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ShouldReadTheStreamFromCheckpoint()
        {
            // Arrange
            const int checkpoint = 10;
            SetupReadStreamBackwards(StreamId, 100);

            // Act
            await Sut.ReplayProjectionFrom(StreamId, checkpoint, (o, token) => Task.CompletedTask,
                CancellationToken.None).ConfigureAwait(false);

            // Assert
            StreamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamForwards(
                    It.Is<StreamId>(streamId => streamId.Value == StreamId),
                    checkpoint,
                    It.IsAny<int>(),
                    It.IsAny<bool>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ShouldFireEventReceivedFunctionWhenEventFound()
        {
            // Arrange
            const int checkpoint = 10;
            SetupReadStreamBackwards(StreamId, 100);

            var fired = false;

            Task EventReceived(object @event, CancellationToken token)
            {
                fired = true;
                return Task.CompletedTask;
            }

            // Act
            await Sut.ReplayProjectionFrom(StreamId, checkpoint, EventReceived, CancellationToken.None).ConfigureAwait(false);

            // Assert
            fired.ShouldBeTrue();
        }

        [Fact]
        public async Task ShouldBeCorrectEvent()
        {
            // Arrange
            SetupReadStreamBackwards(StreamId, 100);

            static Task EventReceived(object @event, CancellationToken token)
            {
                // Assert
                @event.ShouldBeOfType<TestEvent>();
                return Task.CompletedTask;
            }

            // Act
            await Sut.ReplayProjectionFrom(StreamId, StreamVersion.Start, EventReceived, CancellationToken.None).ConfigureAwait(false);
        }
    }

    public class GlobalReplayTests : ReplayManagerTestsFixture
    {
        [Fact]
        public async Task ShouldReadTheStreamFromCheckpoint()
        {
            // Arrange
            const long checkpoint = 10;
            SetupReadAllForwards();

            // Act
            await Sut.ReplayProjectionFrom(checkpoint, (o, token) => Task.CompletedTask,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);

            // Assert
            StreamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadAllForwards(
                    checkpoint,
                    It.IsAny<int>(),
                    It.IsAny<bool>(),
                    CancellationToken.None),
                Times.Once);
        }

        [Fact]
        public async Task ShouldFireEventReceivedFunctionWhenEventFound()
        {
            // Arrange
            SetupReadAllForwards();
            const long checkpoint = 10;
            var fired = false;

            Task EventReceived(object @event, CancellationToken token)
            {
                fired = true;
                return Task.CompletedTask;
            }

            // Act
            await Sut.ReplayProjectionFrom(checkpoint, EventReceived, cancellationToken: CancellationToken.None).ConfigureAwait(false);

            // Assert
            fired.ShouldBeTrue();
        }

        [Fact]
        public async Task ShouldBeCorrectEvent()
        {
            // Arrange
            SetupReadAllForwards();

            static Task EventReceived(object @event, CancellationToken token)
            {
                // Assert
                @event.ShouldBeOfType<TestEvent>();
                return Task.CompletedTask;
            }

            // Act
            await Sut.ReplayProjectionFrom(null, EventReceived, cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
    }
}