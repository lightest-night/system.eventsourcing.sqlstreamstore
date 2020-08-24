using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Checkpoints
{
    public static class CheckpointManager
    {
        private static readonly IDictionary<string, long> Checkpoints = new Dictionary<string, long>();

        public static Task<long?> GetGlobalCheckpoint(CancellationToken cancellationToken = default)
            => Checkpoints.TryGetValue(Constants.GlobalCheckpointId, out var checkpoint)
                ? Task.FromResult((long?) checkpoint)
                : Task.FromResult((long?) null);

        public static Task SetGlobalCheckpoint(long? checkpoint, CancellationToken cancellationToken = default)
        {
            if (checkpoint.HasValue)
                Checkpoints[Constants.GlobalCheckpointId] = checkpoint.Value;
            else
                Checkpoints.Remove(Constants.GlobalCheckpointId);

            return Task.CompletedTask;
        }
    }
}