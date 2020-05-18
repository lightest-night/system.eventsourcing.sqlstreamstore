using System;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.SqlStreamStore.Projections;
using Moq;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Projections
{
    public class CategoryProjectionTests
    {
        private const string AggregateName = "Test";
        private readonly string _streamId = $"{AggregateName}-{Guid.NewGuid()}";
        private readonly NewStreamMessage _streamMessage = new NewStreamMessage(Guid.NewGuid(), "Test", @"{ ""name"": ""test"" }");
        private readonly Mock<IStreamStore> _streamStoreMock = new Mock<IStreamStore>();
        private readonly IEventSourceProjection _sut;

        public CategoryProjectionTests()
        {
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(
                    streamStore => streamStore.ReadStreamBackwards(It.IsAny<StreamId>(), StreamVersion.End, 1, true,
                        It.IsAny<CancellationToken>())
                )
                .ReturnsAsync(new ReadStreamPage(
                    _streamId,
                    PageReadStatus.Success,
                    0,
                    0,
                    ExpectedVersion.NoStream,
                    0,
                    ReadDirection.Backward,
                    true));
            
            _sut = new CategoryProjection(_streamStoreMock.Object);
            CategoryProjection.ClearInternalCache();
        }

        [Fact]
        public async Task ShouldCreateStreamWithCorrectCategoryName()
        {
            // Act
            await _sut.ProcessEvents(_streamId, new[] {_streamMessage}, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            _streamStoreMock.Verify(streamStore => streamStore.AppendToStream(
                $"{Constants.CategoryPrefix}{AggregateName}", It.IsAny<int>(), It.IsAny<NewStreamMessage[]>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ShouldCorrectlyAscertainExpectedVersionWhenANewStream()
        {
            // Act
            await _sut.ProcessEvents(_streamId, new[] {_streamMessage}, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                    It.IsAny<StreamId>(),
                    StreamVersion.End, 
                    1, 
                    true, 
                    It.IsAny<CancellationToken>())
                , Times.Once);
            _streamStoreMock.Verify(streamStore => streamStore.AppendToStream(It.IsAny<StreamId>(), ExpectedVersion.NoStream, It.IsAny<NewStreamMessage[]>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ShouldCorrectlyAscertainExpectedVersionWhenAnExistingStream()
        {
            // Arrange
            var currentVersion = new Random().Next(0, 100);
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(streamStore =>
                    streamStore.ReadStreamBackwards(It.IsAny<StreamId>(), StreamVersion.End, 1, true, It.IsAny<CancellationToken>())
                    )
                .ReturnsAsync(new ReadStreamPage(
                    _streamId,
                    PageReadStatus.Success,
                    StreamVersion.End,
                    0,
                    currentVersion,
                    0,
                    ReadDirection.Backward,
                    true));
            
            // Act
            await _sut.ProcessEvents(_streamId, new[] {_streamMessage}, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                It.IsAny<StreamId>(),
                StreamVersion.End, 
                1, 
                true, 
                It.IsAny<CancellationToken>())
            , Times.Once);
            _streamStoreMock.Verify(streamStore => streamStore.AppendToStream(It.IsAny<StreamId>(), currentVersion, It.IsAny<NewStreamMessage[]>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ShouldUtiliseLocalCachingToResolveDeadlocksOnVersions()
        {
            // Arrange
            var currentVersion = new Random().Next(0, 100);
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(streamStore =>
                    streamStore.ReadStreamBackwards(It.IsAny<StreamId>(), StreamVersion.End, 1, true, It.IsAny<CancellationToken>())
                )
                .ReturnsAsync(new ReadStreamPage(
                    _streamId,
                    PageReadStatus.Success,
                    StreamVersion.End,
                    0,
                    currentVersion,
                    0,
                    ReadDirection.Backward,
                    true));
            
            // Act
            await _sut.ProcessEvents(_streamId, new[] {_streamMessage}, CancellationToken.None).ConfigureAwait(false);
            await _sut.ProcessEvents(_streamId, new[] {_streamMessage}, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            _streamStoreMock.As<IReadonlyStreamStore>().Verify(streamStore => streamStore.ReadStreamBackwards(
                    It.IsAny<StreamId>(),
                    StreamVersion.End, 
                    1, 
                    true, 
                    It.IsAny<CancellationToken>())
                , Times.Once);
            _streamStoreMock.Verify(streamStore => streamStore.AppendToStream(It.IsAny<StreamId>(), currentVersion, It.IsAny<NewStreamMessage[]>(),
                It.IsAny<CancellationToken>()), Times.Once);
            _streamStoreMock.Verify(streamStore => streamStore.AppendToStream(It.IsAny<StreamId>(), currentVersion + 1, It.IsAny<NewStreamMessage[]>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}