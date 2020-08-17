using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.SqlStreamStore.Serialization;
using Shouldly;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class StreamMessageExtensionsTests
    {
        private static readonly Fixture Fixture = new Fixture();
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
                SerializerFactory.Get.Serialize(new Dictionary<string, object> {{Constants.VersionKey, 0}, {"FindMe", true}}),
                (token => Task.FromResult(SerializerFactory.Get.Serialize(Fixture.Create<TestEvent>())))
            );

        [Theory]
        [InlineData(Constants.VersionKey, 0, true)]
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
        public async Task ShouldSuccessfullyMapToAnEventSourcingEventObject()
        {
            // Act
            var result = await _streamMessage.ToEvent(CancellationToken.None);
            
            // Assert
            result.ShouldBeAssignableTo<EventSourceEvent>();
        }

        [Theory]
        [InlineData("TestStream", false)]
        [InlineData(Constants.SystemStreamPrefix + "TestStream", true)]
        [InlineData("Test" + Constants.SystemStreamPrefix + "Stream", false)]
        [InlineData("TestStream" + Constants.SystemStreamPrefix, false)]
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