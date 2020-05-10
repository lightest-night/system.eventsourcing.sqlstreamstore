using System;
using System.Collections.Generic;
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
    public class PersistentSubscriptionManagerTests
    {
        private readonly Mock<IStreamStore> _streamStoreMock = new Mock<IStreamStore>();
        private readonly Mock<IReplayManager> _replayManagerMock = new Mock<IReplayManager>();
        private readonly Mock<GetEventTypes> _getEventTypesMock = new Mock<GetEventTypes>();
        private readonly Mock<IHostApplicationLifetime> _applicationLifetimeMock = new Mock<IHostApplicationLifetime>();
        private readonly EventSourcingOptions _options = new EventSourcingOptions();
        private readonly IPersistentSubscriptionManager _sut;

        protected PersistentSubscriptionManagerTests()
        {
            var observableMock = new Mock<IObservable<Unit>>();
            observableMock.Setup(observable => observable.Subscribe(It.IsAny<IObserver<Unit>>()))
                .Returns(new TestDisposable());
            _streamStoreMock.As<IReadonlyStreamStore>().Setup(streamStore => streamStore.SubscribeToStream(
                    It.IsAny<StreamId>(),
                    It.IsAny<int?>(),
                    It.IsAny<StreamMessageReceived>(),
                    It.IsAny<SubscriptionDropped>(),
                    It.IsAny<HasCaughtUp>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>()))
                .Returns((StreamId s, int? c, StreamMessageReceived r, SubscriptionDropped d, HasCaughtUp h, bool p, string n) => new StreamSubscription(
                    s, c, _streamStoreMock.Object, observableMock.Object, r, d, h, p, n));

            _getEventTypesMock.Setup(getEventTypes => getEventTypes()).Returns(new[] {typeof(TestEvent)});

            _sut = new PersistentSubscriptionManager(_streamStoreMock.Object,
                _replayManagerMock.Object,
                _getEventTypesMock.Object,
                _applicationLifetimeMock.Object,
                Options.Create(_options),
                Mock.Of<ILogger<PersistentSubscriptionManager>>());
        }
        
        public class CategorySubscriptionTests : PersistentSubscriptionManagerTests
        {
            private readonly string _categoryName = "TestCategory".GetCategoryStreamId();
            
            public CategorySubscriptionTests()
            {
                SetupReadStreamBackwards(_categoryName);
                SetupReadStreamBackwards(new StreamId(_categoryName).GetCheckpointStreamId());
                SetupReadStreamForwards(_categoryName);
            }
            
            [Fact]
            public async Task Should_Get_Last_Checkpoint_Version_When_Creating_New_Category_Subscription()
            {
                // Act
                await _sut.CreateCategorySubscription(_categoryName, (o, token) => Task.CompletedTask,
                    CancellationToken.None);

                // Assert
                var expectedStreamId = new StreamId(_categoryName).GetCheckpointStreamId();
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                        It.Is<StreamId>(streamId => streamId.Value == expectedStreamId.Value),
                        StreamVersion.End,
                        1,
                        true,
                        It.IsAny<CancellationToken>())
                    , Times.Once);
            }

            [Fact]
            public async Task Should_Catch_Up_Subscription_If_Far_Behind_When_Creating_New_Category_Subscription()
            {
                // Arrange
                SetupReadStreamBackwards(_categoryName, _options.SubscriptionCheckpointDelta * 2);
                
                // Act
                await _sut.CreateCategorySubscription(_categoryName, (o, token) => Task.CompletedTask,
                    CancellationToken.None);

                // Assert
                _replayManagerMock.Verify(replayManager => replayManager.ReplayProjectionFrom(
                        _categoryName,
                        0,
                        It.IsAny<Func<object, CancellationToken, Task>>(),
                        It.IsAny<CancellationToken>()),
                    Times.Once);
            }

            [Fact]
            public async Task Should_Subscribe_To_Category_Stream_When_Creating_New_Category_Subscription()
            {
                // Act
                var subscriptionId = await _sut.CreateCategorySubscription(_categoryName, (o, token) => Task.CompletedTask, CancellationToken.None);
                
                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.SubscribeToStream(
                    It.Is<StreamId>(streamId => streamId.Value == _categoryName),
                    null,
                    It.IsAny<StreamMessageReceived>(),
                    It.IsAny<SubscriptionDropped>(),
                    It.IsAny<HasCaughtUp>(), 
                    It.IsAny<bool>(),
                    It.Is<string>(name => name.Contains(subscriptionId.ToString()))),
                    Times.Once);
            }

            [Fact]
            public async Task Should_Remove_Checkpoint_Stream_If_Empty_And_Error_Occurs_When_Creating_New_Category_Subscription()
            {
                // Arrange
                SetupReadStreamBackwards(_categoryName, 100);
                _getEventTypesMock.Setup(getEventTypes => getEventTypes()).Returns(new Type[0]);
                _replayManagerMock.Setup(replayManager => replayManager.ReplayProjectionFrom(_categoryName,
                        It.IsAny<int>(), It.IsAny<Func<object, CancellationToken, Task>>(),
                        It.IsAny<CancellationToken>()))
                    .Throws(new Exception());
                
                // Act
                await Should.ThrowAsync<Exception>(_sut.CreateCategorySubscription(_categoryName, (o, token) => Task.CompletedTask, CancellationToken.None));
                
                // Assert
                var checkpointStreamId = new StreamId(_categoryName).GetCheckpointStreamId();
                _streamStoreMock.Verify(streamStore => streamStore.DeleteStream(
                    It.Is<StreamId>(streamId => streamId.Value == checkpointStreamId.Value),
                    It.IsAny<int>(),
                    It.IsAny<CancellationToken>()),
                    Times.Once);
            }
        }

        public class CloseSubscriptionTests : PersistentSubscriptionManagerTests
        {
            private readonly string _streamName = "CloseTest".GetCategoryStreamId();

            public CloseSubscriptionTests()
            {
                SetupReadStreamBackwards(_streamName);
                SetupReadStreamBackwards(new StreamId(_streamName).GetCheckpointStreamId());
                SetupReadStreamForwards(_streamName);
            }

            [Fact]
            public void Should_Not_Error_If_Nothing_To_Do()
            {
                // Act
                Should.NotThrow(async () => await _sut.CloseSubscription(Guid.NewGuid(), CancellationToken.None));
            }

            [Fact]
            public async Task Should_Delete_Subscription_Checkpoint_Stream()
            {
                // Arrange
                var subscriptionId = await _sut.CreateCategorySubscription(_streamName, (o, token) => Task.CompletedTask, CancellationToken.None);
                
                // Act
                await _sut.CloseSubscription(subscriptionId, CancellationToken.None);
                
                // Assert
                _streamStoreMock.Verify(streamStore => streamStore.DeleteStream(
                    It.Is<StreamId>(streamId => streamId.Value == new StreamId(_streamName).GetCheckpointStreamId()),
                    It.IsAny<int>(),
                    It.IsAny<CancellationToken>()),
                    Times.Once);
            }
        }

        public class CheckpointTests : PersistentSubscriptionManagerTests
        {
            private const int Checkpoint = 10;
            private const string StreamId = "Test_Stream";
            private readonly StreamId _checkpointStreamId = new StreamId(StreamId).GetCheckpointStreamId();

            public CheckpointTests()
            {
                SetupReadStreamBackwards(_checkpointStreamId);
            }

            [Fact]
            public async Task Should_Get_Stream_Metadata()
            {
                // Arrange
                _streamStoreMock.Setup(streamStore => streamStore.GetStreamMetadata(_checkpointStreamId, It.IsAny<CancellationToken>()))
                    .Verifiable();
                
                // Act
                await _sut.SaveCheckpoint(Checkpoint, StreamId);
                
                // Assert
                _streamStoreMock.Verify();
            }
            
            [Fact]
            public async Task Should_Set_Stream_Metadata_When_A_New_Stream()
            {
                // Act
                await _sut.SaveCheckpoint(Checkpoint, StreamId);
                
                // Assert
                _streamStoreMock.Verify(
                    streamStore => streamStore.SetStreamMetadata(_checkpointStreamId, It.IsAny<int>(), It.IsAny<int?>(),
                        1, It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
            }

            [Fact]
            public async Task Should_Not_Set_Stream_Metadata_If_Already_Set()
            {
                // Arrange
                _streamStoreMock.Setup(streamStore =>
                        streamStore.GetStreamMetadata(_checkpointStreamId, It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new StreamMetadataResult(StreamId, 1));
                
                // Act
                await _sut.SaveCheckpoint(Checkpoint, StreamId);
                
                // Assert
                _streamStoreMock.Verify(
                    streamStore => streamStore.SetStreamMetadata(It.IsAny<StreamId>(), It.IsAny<int>(),
                        It.IsAny<int?>(),
                        It.IsAny<int?>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
            }

            [Fact]
            public async Task Should_Save_Checkpoint_Into_Stream()
            {
                // Act
                await _sut.SaveCheckpoint(Checkpoint, StreamId);
                
                // Assert
                _streamStoreMock.Verify(
                    streamStore => streamStore.AppendToStream(
                        It.Is<StreamId>(streamId => streamId.Value == _checkpointStreamId.Value),
                        ExpectedVersion.NoStream + 1,
                        It.Is<NewStreamMessage[]>(messages =>
                            messages.Any(message => message.JsonData == Checkpoint.ToString())),
                        It.IsAny<CancellationToken>()), Times.Once);
            }

            [Fact]
            public async Task Should_Save_Global_Checkpoint()
            {
                // Arrange
                var checkpointStreamId = new StreamId(Constants.GlobalCheckpointId).GetCheckpointStreamId();
                SetupReadStreamBackwards(checkpointStreamId);
                
                // Act
                await _sut.SaveGlobalCheckpoint(Checkpoint);
                
                // Assert
                _streamStoreMock.Verify(
                    streamStore => streamStore.AppendToStream(
                        It.Is<StreamId>(streamId => streamId.Value == checkpointStreamId),
                        It.IsAny<int>(),
                        It.Is<NewStreamMessage[]>(messages =>
                            messages.Any(message => message.JsonData == Checkpoint.ToString())),
                        It.IsAny<CancellationToken>()), Times.Once);
            }

            [Fact]
            public async Task Should_Get_Global_Checkpoint()
            {
                // Arrange
                SetupReadStreamBackwards(new StreamId(Constants.GlobalCheckpointId).GetCheckpointStreamId());
                await _sut.SaveGlobalCheckpoint(Checkpoint);
                
                // Act
                var globalCheckpoint = _sut.GlobalCheckpoint;
                
                // Assert
                globalCheckpoint.ShouldBe(Checkpoint);
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
    }
}