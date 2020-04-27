using System;
using Shouldly;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class ExtendsStringTests
    {
        [Fact]
        public void Should_Get_Category_StreamId()
        {
            // Arrange
            var streamId = Guid.NewGuid().ToString();
            
            // Act
            var result = streamId.GetCategoryStreamId();
            
            // Assert
            result.ShouldBe($"{Constants.CategoryPrefix}{streamId}");
        }
    }
}