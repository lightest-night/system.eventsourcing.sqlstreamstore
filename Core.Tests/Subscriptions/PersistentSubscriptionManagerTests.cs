using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
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

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Subscriptions
{
    public class PersistentSubscriptionManagerTests
    {
        private readonly Mock<IStreamStore> _streamStoreMock = new Mock<IStreamStore>();
        private readonly Mock<GetEventTypes> _getEventTypesMock = new Mock<GetEventTypes>();
        private readonly Mock<IHostApplicationLifetime> _applicationLifetimeMock = new Mock<IHostApplicationLifetime>();
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
                Mock.Of<ILogger<PersistentSubscriptionManager>>(),
                _getEventTypesMock.Object,
                _applicationLifetimeMock.Object,
                Mock.Of<IOptions<EventSourcingOptions>>());
        }

        public class CategorySubscriptionTests : PersistentSubscriptionManagerTests
        {
            private const string CategoryName = "TestCategory";
            
            public CategorySubscriptionTests()
            {
                SetupReadStreamBackwards(CategoryName);
                SetupReadStreamBackwards(new StreamId(CategoryName).GetCheckpointStreamId());
                SetupReadStreamForwards(CategoryName);
            }
            
            [Fact]
            public async Task Should_Get_Last_Checkpoint_Version_When_Creating_New_Category_Subscription()
            {
                // Act
                await _sut.CreateCategorySubscription(CategoryName, (o, token) => Task.CompletedTask,
                    CancellationToken.None);

                // Assert
                var expectedStreamId = new StreamId(CategoryName).GetCheckpointStreamId();
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
                SetupReadStreamBackwards(CategoryName, 100);
                
                // Act
                await _sut.CreateCategorySubscription(CategoryName, (o, token) => Task.CompletedTask,
                    CancellationToken.None);

                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamForwards(
                    It.Is<StreamId>(streamId => streamId.Value == CategoryName),
                    0,
                    It.IsAny<int>(),
                    It.IsAny<bool>(),
                    It.IsAny<CancellationToken>()), Times.Once);
            }

            [Fact]
            public async Task Should_Subscribe_To_Category_Stream_When_Creating_New_Category_Subscription()
            {
                // Act
                var subscriptionId = await _sut.CreateCategorySubscription(CategoryName, (o, token) => Task.CompletedTask, CancellationToken.None);
                
                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.SubscribeToStream(
                    It.Is<StreamId>(streamId => streamId.Value == CategoryName),
                    0,
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
                SetupReadStreamBackwards(CategoryName, 100);
                _getEventTypesMock.Setup(getEventTypes => getEventTypes()).Returns(new Type[0]);
                
                // Act
                await Should.ThrowAsync<InvalidOperationException>(() => _sut.CreateCategorySubscription(CategoryName, (o, token) => Task.CompletedTask, CancellationToken.None));
                
                // Assert
                var checkpointStreamId = new StreamId(CategoryName).GetCheckpointStreamId();
                _streamStoreMock.Verify(streamStore => streamStore.DeleteStream(
                    It.Is<StreamId>(streamId => streamId.Value == checkpointStreamId.Value),
                    It.IsAny<int>(),
                    It.IsAny<CancellationToken>()),
                    Times.Once);
            }
        }

        public class CatchUpTests : PersistentSubscriptionManagerTests
        {
            private const string StreamName = "CatchUpTest";
            
            public CatchUpTests()
            {
                SetupReadStreamBackwards(StreamName);
                SetupReadStreamForwards(StreamName);
            }
            
            [Fact]
            public async Task Should_Get_Last_Version_When_Catching_Up()
            {
                // Act
                await _sut.CatchSubscriptionUp(StreamName, 0, (o, token) => Task.CompletedTask, CancellationToken.None);
                
                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                        It.Is<StreamId>(streamId => streamId.Value == StreamName),
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
                SetupReadStreamBackwards(StreamName, 100);
                
                // Act
                await _sut.CatchSubscriptionUp(StreamName, checkpoint, (o, token) => Task.CompletedTask, CancellationToken.None);
                
                // Assert
                _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamForwards(
                    It.Is<StreamId>(streamId => streamId.Value == StreamName),
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
                SetupReadStreamBackwards(StreamName, 100);

                var fired = false;
                Task EventReceived(object o, CancellationToken token)
                {
                    fired = true;
                    return Task.CompletedTask;
                }

                // Act
                await _sut.CatchSubscriptionUp(StreamName, checkpoint, EventReceived, CancellationToken.None);
                
                // Assert
                fired.ShouldBeTrue();
            }
        }

        public class CloseSubscriptionTests : PersistentSubscriptionManagerTests
        {
            private const string StreamName = "CloseTest";

            public CloseSubscriptionTests()
            {
                SetupReadStreamBackwards(StreamName);
                SetupReadStreamBackwards(new StreamId(StreamName).GetCheckpointStreamId());
                SetupReadStreamForwards(StreamName);
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
                var subscriptionId = await _sut.CreateCategorySubscription(StreamName, (o, token) => Task.CompletedTask, CancellationToken.None);
                
                // Act
                await _sut.CloseSubscription(subscriptionId, CancellationToken.None);
                
                // Assert
                _streamStoreMock.Verify(streamStore => streamStore.DeleteStream(
                    It.Is<StreamId>(streamId => streamId.Value == new StreamId(StreamName).GetCheckpointStreamId()),
                    It.IsAny<int>(),
                    It.IsAny<CancellationToken>()),
                    Times.Once);
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