using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Replay;
using LightestNight.System.EventSourcing.SqlStreamStore.Subscriptions;
using LightestNight.System.EventSourcing.Subscriptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Infrastructure;
using SqlStreamStore.Streams;
using SqlStreamStore.Subscriptions;
using Xunit;
// ReSharper disable ExplicitCallerInfoArgument

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Subscriptions
{
    public abstract class PersistentSubscriptionManagerTestsFixture
    {
        protected Mock<IStreamStore> StreamStoreMock { get; }
        protected Mock<IReplayManager> ReplayManagerMock { get; }
        protected Mock<GetEventTypes> GetEventTypesMock { get; }
        private Mock<IHostApplicationLifetime> ApplicationLifetimeMock { get; }
        protected EventSourcingOptions EventSourcingOptions { get; }
        protected IPersistentSubscriptionManager Sut { get; }

        [SuppressMessage("ReSharper", "CA2000")]
        protected PersistentSubscriptionManagerTestsFixture()
        {
            StreamStoreMock = new Mock<IStreamStore>();
            ReplayManagerMock = new Mock<IReplayManager>();
            GetEventTypesMock = new Mock<GetEventTypes>();
            ApplicationLifetimeMock = new Mock<IHostApplicationLifetime>();
            EventSourcingOptions = new EventSourcingOptions();
            
            var observableMock = new Mock<IObservable<Unit>>();
            observableMock.Setup(observable => observable.Subscribe(It.IsAny<IObserver<Unit>>()))
                .Returns(new TestDisposable());
            StreamStoreMock.As<IReadonlyStreamStore>().Setup(streamStore => streamStore.SubscribeToStream(
                    It.IsAny<StreamId>(),
                    It.IsAny<int?>(),
                    It.IsAny<StreamMessageReceived>(),
                    It.IsAny<SubscriptionDropped>(),
                    It.IsAny<HasCaughtUp>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>()))
                .Returns((StreamId s, int? c, StreamMessageReceived r, SubscriptionDropped d, HasCaughtUp h, bool p, string n) => new StreamSubscription(
                    s, c, StreamStoreMock.Object, observableMock.Object, r, d, h, p, n));

            GetEventTypesMock.Setup(getEventTypes => getEventTypes()).Returns(new[] {typeof(TestEvent)});
            
            SetupReadStreamBackwards(new StreamId(Constants.GlobalCheckpointId).GetCheckpointStreamId());

            Sut = new PersistentSubscriptionManager(StreamStoreMock.Object,
                ReplayManagerMock.Object,
                GetEventTypesMock.Object,
                ApplicationLifetimeMock.Object,
                Options.Create(EventSourcingOptions),
                Mock.Of<ILogger<PersistentSubscriptionManager>>());
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

        protected void SetupReadStreamForwards(string streamId)
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
                            JsonSerializer.Serialize(new TestEvent {Id = Guid.NewGuid()}))
                    }
                ));
        }
    }

    public class CategorySubscriptionTests : PersistentSubscriptionManagerTestsFixture
    {
        private readonly string _categoryName = "TestCategory".GetCategoryStreamId();

        public CategorySubscriptionTests()
        {
            SetupReadStreamBackwards(_categoryName);
            SetupReadStreamBackwards(new StreamId(_categoryName).GetCheckpointStreamId());
            SetupReadStreamForwards(_categoryName);
        }

        [Fact]
        public async Task ShouldGetLastCheckpointVersionWhenCreatingNewCategorySubscription()
        {
            // Act
            await Sut.CreateCategorySubscription(_categoryName, (o, position, version, token) => Task.CompletedTask,
                CancellationToken.None).ConfigureAwait(false);

            // Assert
            var expectedStreamId = new StreamId(_categoryName).GetCheckpointStreamId();
            StreamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                    It.Is<StreamId>(streamId => streamId.Value == expectedStreamId.Value),
                    StreamVersion.End,
                    1,
                    true,
                    It.IsAny<CancellationToken>())
                , Times.Once);
        }

        [Fact]
        public async Task ShouldCatchUpSubscriptionIfFarBehindWhenCreatingNewCategorySubscription()
        {
            // Arrange
            SetupReadStreamBackwards(_categoryName, EventSourcingOptions.SubscriptionCheckpointDelta * 2);

            // Act
            await Sut.CreateCategorySubscription(_categoryName, (o, position, version, token) => Task.CompletedTask,
                CancellationToken.None).ConfigureAwait(false);

            // Assert
            ReplayManagerMock.Verify(replayManager => replayManager.ReplayProjectionFrom(
                    _categoryName,
                    0,
                    It.IsAny<EventReceived>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [Fact]
        public async Task ShouldSubscribeToCategoryStreamWhenCreatingNewCategorySubscription()
        {
            // Act
            var subscriptionId = await Sut.CreateCategorySubscription(_categoryName,
                (o, position, version, token) => Task.CompletedTask, CancellationToken.None).ConfigureAwait(false);

            // Assert
            StreamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.SubscribeToStream(
                    It.Is<StreamId>(streamId => streamId.Value == _categoryName),
                    null,
                    It.IsAny<StreamMessageReceived>(),
                    It.IsAny<SubscriptionDropped>(),
                    It.IsAny<HasCaughtUp>(),
                    It.IsAny<bool>(),
                    It.Is<string>(name => name.Contains(subscriptionId.ToString(), StringComparison.InvariantCulture))),
                Times.Once);
        }

        [Fact]
        public async Task ShouldRemoveCheckpointStreamIfEmptyAndErrorOccursWhenCreatingNewCategorySubscription()
        {
            // Arrange
            SetupReadStreamBackwards(_categoryName, 100);
            GetEventTypesMock.Setup(getEventTypes => getEventTypes()).Returns(Array.Empty<Type>());
            ReplayManagerMock.Setup(replayManager => replayManager.ReplayProjectionFrom(_categoryName,
                    It.IsAny<int>(), It.IsAny<EventReceived>(),
                    It.IsAny<CancellationToken>()))
                .Throws(new Exception());

            // Act
            await Should.ThrowAsync<Exception>(Sut.CreateCategorySubscription(_categoryName,
                (o, position, version, token) => Task.CompletedTask, CancellationToken.None)).ConfigureAwait(false);

            // Assert
            var checkpointStreamId = new StreamId(_categoryName).GetCheckpointStreamId();
            StreamStoreMock.Verify(streamStore => streamStore.DeleteStream(
                    It.Is<StreamId>(streamId => streamId.Value == checkpointStreamId.Value),
                    It.IsAny<int>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }
    }

    public class CloseSubscriptionTests : PersistentSubscriptionManagerTestsFixture
    {
        private readonly string _streamName = "CloseTest".GetCategoryStreamId();
    
        public CloseSubscriptionTests()
        {
            SetupReadStreamBackwards(_streamName);
            SetupReadStreamBackwards(new StreamId(_streamName).GetCheckpointStreamId());
            SetupReadStreamForwards(_streamName);
        }
    
        [Fact]
        public void ShouldNotErrorIfNothingToDo()
        {
            // Act
            Should.NotThrow(async () => await Sut.CloseSubscription(Guid.NewGuid(), CancellationToken.None).ConfigureAwait(false));
        }
    
        [Fact]
        public async Task ShouldDeleteSubscriptionCheckpointStream()
        {
            // Arrange
            var subscriptionId = await Sut.CreateCategorySubscription(_streamName,
                (o, position, version, token) => Task.CompletedTask, CancellationToken.None).ConfigureAwait(false);
    
            // Act
            await Sut.CloseSubscription(subscriptionId, CancellationToken.None).ConfigureAwait(false);
    
            // Assert
            StreamStoreMock.Verify(streamStore => streamStore.DeleteStream(
                    It.Is<StreamId>(streamId => streamId.Value == new StreamId(_streamName).GetCheckpointStreamId()),
                    It.IsAny<int>(),
                    It.IsAny<CancellationToken>()),
                Times.Once);
        }
    }
    
    public class CheckpointTests : PersistentSubscriptionManagerTestsFixture
    {
        private const int Checkpoint = 10;
        private const string StreamId = "Test_Stream";
        private readonly StreamId _checkpointStreamId = new StreamId(StreamId).GetCheckpointStreamId();
    
        public CheckpointTests()
        {
            SetupReadStreamBackwards(_checkpointStreamId);
        }
        
        [Fact]
        public async Task ShouldGetStreamMetadata()
        {
            // Arrange
            StreamStoreMock.Setup(streamStore =>
                    streamStore.GetStreamMetadata(_checkpointStreamId, It.IsAny<CancellationToken>()))
                .Verifiable();
    
            // Act
            await Sut.SaveCheckpoint(Checkpoint, StreamId).ConfigureAwait(false);
    
            // Assert
            StreamStoreMock.Verify();
        }
    
        [Fact]
        public async Task ShouldSetStreamMetadataWhenANewStream()
        {
            // Act
            await Sut.SaveCheckpoint(Checkpoint, StreamId).ConfigureAwait(false);
    
            // Assert
            StreamStoreMock.Verify(
                streamStore => streamStore.SetStreamMetadata(_checkpointStreamId, It.IsAny<int>(), It.IsAny<int?>(),
                    1, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        }
    
        [Fact]
        public async Task ShouldNotSetStreamMetadataIfAlreadySet()
        {
            // Arrange
            StreamStoreMock.Setup(streamStore =>
                    streamStore.GetStreamMetadata(_checkpointStreamId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new StreamMetadataResult(StreamId, 1));
    
            // Act
            await Sut.SaveCheckpoint(Checkpoint, StreamId).ConfigureAwait(false);
    
            // Assert
            StreamStoreMock.Verify(
                streamStore => streamStore.SetStreamMetadata(It.Is<StreamId>(streamId => streamId.Value == StreamId),
                    It.IsAny<int>(),
                    It.IsAny<int?>(),
                    It.IsAny<int?>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }
    
        [Fact]
        public async Task ShouldSaveCheckpointIntoStream()
        {
            // Act
            await Sut.SaveCheckpoint(Checkpoint, StreamId).ConfigureAwait(false);
    
            // Assert
            StreamStoreMock.Verify(
                streamStore => streamStore.AppendToStream(
                    It.Is<StreamId>(streamId => streamId.Value == _checkpointStreamId.Value),
                    ExpectedVersion.NoStream + 1,
                    It.Is<NewStreamMessage[]>(messages =>
                        messages.Any(message => message.JsonData == Checkpoint.ToString(CultureInfo.InvariantCulture))),
                    It.IsAny<CancellationToken>()), Times.Once);
        }
    
        [Fact]
        public async Task ShouldSaveGlobalCheckpoint()
        {
            // Arrange
            var checkpointStreamId = new StreamId(Constants.GlobalCheckpointId).GetCheckpointStreamId();
            SetupReadStreamBackwards(checkpointStreamId);
    
            // Act
            await Sut.SaveGlobalCheckpoint(Checkpoint).ConfigureAwait(false);
    
            // Assert
            StreamStoreMock.Verify(
                streamStore => streamStore.AppendToStream(
                    It.Is<StreamId>(streamId => streamId.Value == checkpointStreamId),
                    It.IsAny<int>(),
                    It.Is<NewStreamMessage[]>(messages =>
                        messages.Any(message => message.JsonData == Checkpoint.ToString(CultureInfo.InvariantCulture))),
                    It.IsAny<CancellationToken>()), Times.Once);
        }
    
        [Fact]
        public async Task ShouldGetGlobalCheckpoint()
        {
            // Arrange
            SetupReadStreamBackwards(new StreamId(Constants.GlobalCheckpointId).GetCheckpointStreamId());
            await Sut.SaveGlobalCheckpoint(Checkpoint).ConfigureAwait(false);
    
            // Act
            var globalCheckpoint = Sut.GlobalCheckpoint;
    
            // Assert
            globalCheckpoint.ShouldBe(Checkpoint);
        }
    }
}