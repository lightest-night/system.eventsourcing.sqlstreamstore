using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Checkpoints;
using LightestNight.System.EventSourcing.SqlStreamStore.Checkpoints;
using Shouldly;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Checkpoints
{
    public class CheckpointManagerTests
    {
        private readonly ICheckpointManager _sut;

        public CheckpointManagerTests()
        {
            _sut = new CheckpointManager();
        }

        [Fact]
        public async Task ShouldReturnNullWhenCheckpointDoesNotExist()
        {
            // Act
            var result = await _sut.GetCheckpoint(string.Empty, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBeNull();
        }

        [Fact]
        public async Task ShouldReturnCheckpoint()
        {
            // Arrange
            const string checkpointId = "Checkpoint";
            const int checkpoint = 100;
            var checkpoints = typeof(CheckpointManager).GetField("Checkpoints", BindingFlags.Static | BindingFlags.NonPublic)?.GetValue(_sut) as IDictionary<string, long>;
            checkpoints?.Add(checkpointId, checkpoint);
            
            // Act
            var result = await _sut.GetCheckpoint(checkpointId, CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBe(checkpoint);
        }
        
        [Fact]
        public async Task ShouldReturnNullWhenGlobalCheckpointNotSet()
        {
            // Act
            var result = await _sut.GetGlobalCheckpoint(CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBeNull();
        }

        [Fact]
        public async Task ShouldReturnGlobalCheckpoint()
        {
            // Arrange
            const int checkpoint = 100;
            var checkpoints = typeof(CheckpointManager).GetField("Checkpoints", BindingFlags.Static | BindingFlags.NonPublic)?.GetValue(_sut) as IDictionary<string, long>;
            checkpoints?.Add(Constants.GlobalCheckpointId, checkpoint);
            
            // Act
            var result = await _sut.GetGlobalCheckpoint(CancellationToken.None).ConfigureAwait(false);
            
            // Assert
            result.ShouldBe(checkpoint);
        }
    }
}