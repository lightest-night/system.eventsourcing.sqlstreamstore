using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsStreamId
    {
        public static StreamId GetCheckpointStreamId(this StreamId target)
            => new StreamId($"{(target.Value.StartsWith(Constants.CategoryPrefix) ? string.Empty : Constants.CategoryPrefix)}{Constants.CheckpointPrefix}-{target}");

        public static StreamId GetCategoryStreamId(this StreamId target)
            => new StreamId($"{(target.Value.StartsWith(Constants.CategoryPrefix) ? string.Empty : Constants.CategoryPrefix)}{target}");
    }
}