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
        public void Should_Add_The_ce_Prefix()
        {
            // Arrange
            StreamId streamId = Guid.NewGuid().ToString();
            
            // Act
            var result = streamId.GetCheckpointStreamId();
            
            // Assert
            result.Value.ShouldBe($"ce-{Constants.CheckpointPrefix}-{streamId.Value}");
        }
    }
}