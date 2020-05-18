using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Checkpoints;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Checkpoints
{
    public class CheckpointManager : ICheckpointManager
    {
        private static readonly IDictionary<string, long> Checkpoints = new Dictionary<string, long>();

        public Task<int?> GetCheckpoint(string checkpointId, CancellationToken cancellationToken = default)
            => Checkpoints.TryGetValue(checkpointId, out var checkpoint)
                ? Task.FromResult((int?) checkpoint)
                : Task.FromResult((int?) null);

        public Task<long?> GetGlobalCheckpoint(CancellationToken cancellationToken = default)
            => Checkpoints.TryGetValue(Constants.GlobalCheckpointId, out var checkpoint)
                ? Task.FromResult((long?) checkpoint)
                : Task.FromResult((long?) null);

        public Task SetCheckpoint<TCheckpoint>(string checkpointId, TCheckpoint checkpoint, CancellationToken cancellationToken = default)
        {
            Checkpoints[checkpointId] = Convert.ToInt64(checkpoint, CultureInfo.InvariantCulture);
            return Task.CompletedTask;
        }
    }
}