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
        public async Task Should_Correctly_Ascertain_Expected_Version_When_A_New_Stream()
        {
            // Act
            var result = await _streamStoreMock.Object.GetLastVersionOfStream(StreamName, CancellationToken.None);
            
            // Assert
            result.ShouldBe(ExpectedVersion.NoStream);
        }

        [Fact]
        public async Task Should_Correctly_Return_No_Object_For_GetDataFunc()
        {
            // Act
            var result = await _streamStoreMock.Object.GetLastVersionOfStream<object?>(StreamName, CancellationToken.None);
            
            // Assert
            (await result.GetDataFunc(CancellationToken.None)).ShouldBe(default);
        }
        
        [Fact]
        public async Task Should_Correctly_Ascertain_Expected_Version_When_An_Existing_Stream()
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
            var result = await _streamStoreMock.Object.GetLastVersionOfStream(StreamName, CancellationToken.None);
            
            // Assert
            result.ShouldBe(currentVersion);
        }

        [Fact]
        public async Task Should_Correctly_Return_Message_Contents_For_GetDataFunc_When_Existing_Stream()
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
            var result = await _streamStoreMock.Object.GetLastVersionOfStream<int>(StreamName, CancellationToken.None);
            
            // Assert
            (await result.GetDataFunc(CancellationToken.None)).ShouldBe(expectedResult);
        }
    }
}