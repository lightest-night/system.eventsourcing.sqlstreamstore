using System;
using System.Collections.Generic;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.Utilities;
using LightestNight.System.Utilities.Extensions;
using Newtonsoft.Json;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsEventSourceEvent
    {
        public static NewStreamMessage ToMessageData<TEvent>(this TEvent evt,
            IDictionary<string, object>? headers = null) where TEvent : IEventSourceEvent
        {
            var eventClrType = evt.ThrowIfNull(nameof(evt)).GetType();
            var typeName = EventTypeAttribute.GetEventTypeFrom(eventClrType);
            if (typeName == default)
                throw new ArgumentException("Event Type Name could not be determined", nameof(evt));

            var version = Attributes.GetCustomAttributeValue<EventTypeAttribute, int>(eventClrType,
                eventTypeAttribute => eventTypeAttribute.Version);
            headers ??= new Dictionary<string, object>();
            headers.Add(Constants.VersionKey, version);
            headers.TryAdd(Constants.TimestampKey, new DateTimeOffset(DateTime.UtcNow));

            var data = JsonConvert.SerializeObject(evt, eventClrType, Json.Settings);
            var metadata = JsonConvert.SerializeObject(headers, Json.Settings);

            return new NewStreamMessage(Guid.NewGuid(), typeName, data, metadata);
        }
    }
}