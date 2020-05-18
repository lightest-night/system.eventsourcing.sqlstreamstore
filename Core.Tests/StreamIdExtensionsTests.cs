using System;
using Shouldly;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class StreamIdExtensionsTests
    {
        [Fact]
        public void ShouldReturnCheckpointStreamId()
        {
            // Arrange
            StreamId streamId = $"{Constants.CategoryPrefix}{Guid.NewGuid()}";
            
            // Act
            var result = streamId.GetCheckpointStreamId();
            
            // Assert
            result.Value.ShouldBe($"{Constants.CheckpointPrefix}-{streamId.Value}");
        }

        [Fact]
        public void ShouldAddTheCategoryPrefixWhenGettingACheckpointStreamId()
        {
            // Arrange
            StreamId streamId = Guid.NewGuid().ToString();
            
            // Act
            var result = streamId.GetCheckpointStreamId();
            
            // Assert
            result.Value.ShouldBe($"{Constants.CategoryPrefix}{Constants.CheckpointPrefix}-{streamId.Value}");
        }

        [Fact]
        public void ShouldGetCategoryStreamId()
        {
            // Arrange
            StreamId streamId = Guid.NewGuid().ToString();
            
            // Act
            var result = streamId.GetCategoryStreamId();
            
            // Assert
            result.Value.ShouldBe($"{Constants.CategoryPrefix}{streamId.Value}");
        }
    }
}