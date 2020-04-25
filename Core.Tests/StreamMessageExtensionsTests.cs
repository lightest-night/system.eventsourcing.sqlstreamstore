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
            _streamMessage = new StreamMessage(
                Guid.NewGuid().ToString(),
                Guid.NewGuid(),
                1,
                1,
                DateTime.UtcNow,
                nameof(TestEvent),
                JsonSerializer.Serialize(new Dictionary<string, object>{{Constants.VersionKey, 0}, {"FindMe", true}}),
                (token => Task.FromResult(JsonSerializer.Serialize(new TestEvent {Id = Guid.NewGuid()})))
            );
        }

        [Theory]
        [InlineData(Constants.VersionKey, 0, true)]
        [InlineData("Test", null, false)]
        [InlineData("FindMe", true, true)]
        public void Should_Get_Metadata_From_Message_Successfully(string key, object result, bool found)
        {
            // Act
            var lookupResult = _streamMessage.TryGetEventMetadata(key, out var value);
            
            // Assert
            if (found)
                result.ShouldBe(value);
            lookupResult.ShouldBe(found);
        }
        
        [Fact]
        public async Task Should_Successfully_Map_To_An_IEventSourcingEvent_Object()
        {
            // Arrange
            var types = new[] {typeof(TestEvent)};
            
            // Act
            var result = await _streamMessage.ToEvent(types);
            
            // Assert
            result.ShouldBeAssignableTo<IEventSourceEvent>();
        }
    }
}