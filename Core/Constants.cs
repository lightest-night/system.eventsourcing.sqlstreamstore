namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public class Constants
    {
        public const string SystemStreamPrefix = "@";
        public const string VersionKey = "version";
        public const string CheckpointPrefix = "checkpoint";
        public const string CheckpointMessageType = "checkpoint";
        public const string GlobalCheckpointId = "global";
        public static readonly string CategoryPrefix = $"{SystemStreamPrefix}ce-";
    }
}