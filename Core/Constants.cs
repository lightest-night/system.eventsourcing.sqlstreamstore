using Newtonsoft.Json;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class Constants
    {
        public static JsonSerializerSettings JsonSettings => new JsonSerializerSettings
        {
            DateParseHandling = DateParseHandling.DateTimeOffset,
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind
        };
    }
}