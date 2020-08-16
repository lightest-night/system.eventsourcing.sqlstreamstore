using AutoFixture.Xunit2;
using Shouldly;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class EventSourceEventExtensionTests
    {
        [Theory, AutoData]
        public void ShouldCreateNewStreamMessageSuccessfully(TestEvent @event)
        {
            // Act
            var result = @event.ToMessageData();
            
            // Assert
            result.ShouldNotBeNull();
        }
    }
}