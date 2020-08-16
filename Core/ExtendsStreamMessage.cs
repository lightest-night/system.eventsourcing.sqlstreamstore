using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.SqlStreamStore.Serialization;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsStreamMessage
    {
        private static readonly ISerializer Serializer;

        static ExtendsStreamMessage()
        {
            Serializer = SerializerFactory.Get;
        }
        
        public static async Task<EventSourceEvent> ToEvent(this StreamMessage message, CancellationToken cancellationToken = default)
        {
            var typeName = message.Type;
            var version = 0;
            if (message.TryGetEventMetadata(Constants.VersionKey, out var metaVersion))
                version = Convert.ToInt32(metaVersion, CultureInfo.InvariantCulture);
            var eventType = EventCollection.GetEventType(typeName, version);
            
            if (eventType == default || eventType == null)
                throw new InvalidOperationException($"No Event Type found to deserialize message: {typeName} as version {version}");

            var eventData = await message.GetJsonData(cancellationToken).ConfigureAwait(false);
            var @event = Serializer.Deserialize(eventData, eventType);

            if (@event is EventSourceEvent eventSourceEvent)
                return eventSourceEvent;

            throw new InvalidOperationException(
                $"Event Type found to deserialize message: {typeName} at version {version} is not of IEventSourceEvent.");
        }
        
        public static bool TryGetEventMetadata(this StreamMessage message, string key, out object result)
        {
            var metadata = message.JsonMetadataAs<IDictionary<string, object>>();
            var metaResult = metadata.TryGetValue(key, out var value);
            result = value;

            return metaResult;
        }

        public static bool IsInSystemStream(this StreamMessage message)
            => message.StreamId.StartsWith(Constants.SystemStreamPrefix, StringComparison.InvariantCultureIgnoreCase);
    }
}