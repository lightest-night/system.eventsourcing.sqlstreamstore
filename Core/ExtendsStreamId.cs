using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsStreamId
    {
        public static StreamId GetCheckpointStreamId(this StreamId target)
            => new StreamId($"{(target.Value.StartsWith("ce") ? string.Empty : "ce-")}{Constants.CheckpointPrefix}-{target}");
    }
}