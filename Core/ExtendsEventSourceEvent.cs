using System;
using System.Collections.Generic;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.SqlStreamStore.Serialization;
using LightestNight.System.Utilities;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsEventSourceEvent
    {
        private static readonly ISerializer Serializer;

        static ExtendsEventSourceEvent()
        {
            Serializer = SerializerFactory.Get;
        }
        
        public static NewStreamMessage ToMessageData<TEvent>(this TEvent @event,
            IDictionary<string, object>? headers = null) where TEvent : EventSourceEvent
        {
            var eventClrType = (@event ?? throw new ArgumentNullException(nameof(@event))).GetType();
            var typeName = EventTypeAttribute.GetEventTypeFrom(eventClrType);
            if (typeName == default)
                throw new ArgumentException("Event Type Name could not be determined", nameof(@event));

            var version = Attributes.GetCustomAttributeValue<EventTypeAttribute, int>(eventClrType,
                eventTypeAttribute => eventTypeAttribute.Version);
            headers ??= new Dictionary<string, object>();
            headers.Add(Constants.VersionKey, version);
            headers.TryAdd(Constants.TimestampKey, new DateTimeOffset(DateTime.UtcNow));

            var data = Serializer.Serialize(@event, eventClrType);
            var metadata = Serializer.Serialize(headers);

            return new NewStreamMessage(Guid.NewGuid(), typeName, data, metadata);
        }
    }
}