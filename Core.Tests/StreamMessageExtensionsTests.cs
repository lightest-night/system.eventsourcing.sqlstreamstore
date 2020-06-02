using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using Shouldly;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class StreamMessageExtensionsTests
    {
        private readonly StreamMessage _streamMessage;

        public StreamMessageExtensionsTests()
        {
            _streamMessage = BuildMessage();
        }

        private static StreamMessage BuildMessage(string? streamId = default)
            => new StreamMessage(
                string.IsNullOrEmpty(streamId) ? Guid.NewGuid().ToString() : streamId,
                Guid.NewGuid(),
                1,
                1,
                DateTime.UtcNow,
                nameof(TestEvent),
                JsonSerializer.Serialize(new Dictionary<string, object> {{EventSourcing.Constants.VersionKey, 0}, {"FindMe", true}}),
                (token => Task.FromResult(JsonSerializer.Serialize(new TestEvent(Guid.NewGuid()))))
            );

        [Theory]
        [InlineData(EventSourcing.Constants.VersionKey, 0, true)]
        [InlineData("Test", null, false)]
        [InlineData("FindMe", true, true)]
        public void ShouldGetMetadataFromMessageSuccessfully(string key, object result, bool found)
        {
            // Act
            var lookupResult = _streamMessage.TryGetEventMetadata(key, out var value);
            
            // Assert
            if (found)
                result.ShouldBe(value);
            lookupResult.ShouldBe(found);
        }
        
        [Fact]
        public async Task ShouldSuccessfullyMapToAnIEventSourcingEventObject()
        {
            // Arrange
            var types = new[] {typeof(TestEvent)};
            
            // Act
            var result = await _streamMessage.ToEvent(types).ConfigureAwait(false);
            
            // Assert
            result.ShouldBeAssignableTo<IEventSourceEvent>();
        }

        [Theory]
        [InlineData("TestStream", false)]
        [InlineData(EventSourcing.Constants.SystemStreamPrefix + "TestStream", true)]
        [InlineData("Test" + EventSourcing.Constants.SystemStreamPrefix + "Stream", false)]
        [InlineData("TestStream" + EventSourcing.Constants.SystemStreamPrefix, false)]
        public void ShouldSuccessfullyDetermineIfMessageIsInSystemStream(string streamId, bool expected)
        {
            // Arrange
            var streamMessage = BuildMessage(streamId);
            
            // Act
            var result = streamMessage.IsInSystemStream();
            
            // Assert
            result.ShouldBe(expected);
        }
    }
}