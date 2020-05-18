using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    /// <summary>
    /// You could argue these are superfluous as they just call the framework, but we're checking the correct calls are made in the extension methods
    /// </summary>
    public class StreamStoreExtensionsTests
    {
        private const string StreamName = "Test";
        private readonly Mock<IStreamStore> _streamStoreMock = new Mock<IStreamStore>();

        public StreamStoreExtensionsTests()
        {
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(
                    streamStore => streamStore.ReadStreamBackwards(It.IsAny<StreamId>(), StreamVersion.End, 1, true,
                        It.IsAny<CancellationToken>())
                )
                .ReturnsAsync(new ReadStreamPage(
                    StreamName,
                    PageReadStatus.Success,
                    0,
                    0,
                    ExpectedVersion.NoStream,
                    0,
                    ReadDirection.Backward,
                    true));
        }
        
        [Fact]
        public async Task ShouldCorrectlyAscertainExpectedVersionWhenANewStream()
        {
            // Act
            var result = await _streamStoreMock.Object.GetLastVersionOfStream(StreamName, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBe(ExpectedVersion.NoStream);
        }

        [Fact]
        public async Task ShouldCorrectlyReturnNoObjectForGetDataFunc()
        {
            // Act
            var result = await _streamStoreMock.Object.GetLastVersionOfStream<object?>(StreamName, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            (await result.GetDataFunc(CancellationToken.None).ConfigureAwait(false)).ShouldBe(default);
        }
        
        [Fact]
        public async Task ShouldCorrectlyAscertainExpectedVersionWhenAnExistingStream()
        {
            // Arrange
            var currentVersion = new Random().Next(0, 1000);
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(streamStore => streamStore.ReadStreamBackwards(It.IsAny<StreamId>(), StreamVersion.End, 1, true,
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ReadStreamPage(
                    StreamName,
                    PageReadStatus.Success,
                    StreamVersion.End,
                    0,
                    currentVersion,
                    0,
                    ReadDirection.Backward,
                    true));
            
            // Act
            var result = await _streamStoreMock.Object.GetLastVersionOfStream(StreamName, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBe(currentVersion);
        }

        [Fact]
        public async Task ShouldCorrectlyReturnMessageContentsForGetDataFuncWhenExistingStream()
        {
            // Arrange
            const int expectedResult = 1;
            var currentVersion = new Random().Next(0, 1000);
            _streamStoreMock.As<IReadonlyStreamStore>()
                .Setup(streamStore => streamStore.ReadStreamBackwards(It.IsAny<StreamId>(), StreamVersion.End, 1, true,
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ReadStreamPage(
                    StreamName,
                    PageReadStatus.Success,
                    StreamVersion.End,
                    0,
                    currentVersion,
                    0,
                    ReadDirection.Backward,
                    true,
                    messages: new[]
                    {
                        new StreamMessage(StreamName, Guid.NewGuid(), 1, 1, DateTime.UtcNow, "Test", string.Empty,
                            JsonSerializer.Serialize(expectedResult))
                    }));
            
            // Act
            var result = await _streamStoreMock.Object.GetLastVersionOfStream<int>(StreamName, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            (await result.GetDataFunc(CancellationToken.None).ConfigureAwait(false)).ShouldBe(expectedResult);
        }
    }
}