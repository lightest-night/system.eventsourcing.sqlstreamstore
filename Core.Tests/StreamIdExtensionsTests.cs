using System;
using Shouldly;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class StreamIdExtensionsTests
    {
        [Fact]
        public void Should_Return_Checkpoint_StreamId()
        {
            // Arrange
            StreamId streamId = $"ce-{Guid.NewGuid()}";
            
            // Act
            var result = streamId.GetCheckpointStreamId();
            
            // Assert
            result.Value.ShouldBe($"{Constants.CheckpointPrefix}-{streamId.Value}");
        }

        [Fact]
        public void Should_Add_The_Category_Prefix_When_Getting_A_Checkpoint_StreamId()
        {
            // Arrange
            StreamId streamId = Guid.NewGuid().ToString();
            
            // Act
            var result = streamId.GetCheckpointStreamId();
            
            // Assert
            result.Value.ShouldBe($"{Constants.CategoryPrefix}{Constants.CheckpointPrefix}-{streamId.Value}");
        }

        [Fact]
        public void Should_Get_Category_StreamId()
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