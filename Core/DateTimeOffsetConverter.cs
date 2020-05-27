using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using LightestNight.System.Utilities.Extensions;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public class DateTimeOffsetConverter : JsonConverter<DateTimeOffset>
    {
        public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
            => reader.GetString().ToDateTimeOffset();

        public override void Write(Utf8JsonWriter writer, DateTimeOffset dateTimeValue, JsonSerializerOptions options)
            => writer.ThrowIfNull(nameof(writer)).WriteStringValue(dateTimeValue.Serialize());
    }
}