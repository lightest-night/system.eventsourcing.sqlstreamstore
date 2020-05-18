using System;
using Shouldly;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class ExtendsStringTests
    {
        [Fact]
        public void ShouldGetCategoryStreamId()
        {
            // Arrange
            var streamId = Guid.NewGuid().ToString();
            
            // Act
            var result = streamId.GetCategoryStreamId();
            
            // Assert
            result.ShouldBe($"{Constants.CategoryPrefix}{streamId}");
        }

        [Fact]
        public void ShouldGetCategoryStreamIdEvenWhenPrefixAlreadySet()
        {
            // Arrange
            var streamId = $"{Constants.CategoryPrefix}{Guid.NewGuid()}";
            
            // Act
            var result = streamId.GetCategoryStreamId();
            
            // Assert
            result.ShouldBe(streamId);
        }
    }
}