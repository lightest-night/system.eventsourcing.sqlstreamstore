using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.SqlStreamStore.Checkpoints;
using Shouldly;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Checkpoints
{
    public class CheckpointManagerTests
    {
        public CheckpointManagerTests()
        {
            var checkpoints = typeof(CheckpointManager).GetField("Checkpoints", BindingFlags.Static | BindingFlags.NonPublic)?.GetValue(null) as IDictionary<string, long>;
            checkpoints?.Clear();
        }
        
        [Fact]
        public async Task ShouldReturnNullWhenGlobalCheckpointNotSet()
        {
            // Act
            var result = await CheckpointManager.GetGlobalCheckpoint(CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBeNull();
        }

        [Fact]
        public async Task ShouldReturnGlobalCheckpoint()
        {
            // Arrange
            const int checkpoint = 100;
            var checkpoints = typeof(CheckpointManager).GetField("Checkpoints", BindingFlags.Static | BindingFlags.NonPublic)?.GetValue(null) as IDictionary<string, long>;
            checkpoints?.Add(EventSourcing.Constants.GlobalCheckpointId, checkpoint);
            
            // Act
            var result = await CheckpointManager.GetGlobalCheckpoint(CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBe(checkpoint);
        }

        [Fact]
        public async Task ShouldSetGlobalCheckpoint()
        {
            // Arrange
            const long checkpoint = 100;
            
            // Act
            await CheckpointManager.SetGlobalCheckpoint(checkpoint).ConfigureAwait(false);
            
            // Assert
            var result = await CheckpointManager.GetGlobalCheckpoint().ConfigureAwait(false);
            result.ShouldBe(checkpoint);
        }
        
        [Fact]
        public async Task ShouldSetGlobalCheckpointSuccessfullyWhenNull()
        {
            // Arrange
            long? checkpoint = null;
            
            // Act
            await CheckpointManager.SetGlobalCheckpoint(checkpoint).ConfigureAwait(false);
            
            // Assert
            var result = await CheckpointManager.GetGlobalCheckpoint().ConfigureAwait(false);
            result.ShouldBe(checkpoint);
        }
    }
}