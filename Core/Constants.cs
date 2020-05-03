namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public class Constants
    {
        public const string SystemStreamPrefix = "@";
        public const string VersionKey = "version";
        public const string CheckpointPrefix = "checkpoint";
        public static readonly string CategoryPrefix = $"{SystemStreamPrefix}ce-";
    }
}