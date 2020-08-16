namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization
{
    public enum Serializers : uint
    {
        Unknown = 0,
        Newtonsoft,
        Utf8Json,
        SystemTextJson
    }
}