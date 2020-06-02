﻿using System;
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
            Serializer = SerializerFactory.GetSerializer();
        }
        
        public static async Task<IEventSourceEvent> ToEvent(this StreamMessage message, IEnumerable<Type> eventTypes,
            CancellationToken cancellationToken = default)
        {
            var typeName = message.Type;
            var version = 0;
            if (message.TryGetEventMetadata(Constants.VersionKey, out var metaVersion))
                version = Convert.ToInt32(metaVersion, CultureInfo.InvariantCulture);
            var eventType = eventTypes.GetEventType(typeName, version);
            
            if (eventType == default || eventType == null)
                throw new InvalidOperationException($"No Event Type found to deserialize message: {typeName} as version {version}");

            var eventDataTask = message.GetJsonData(cancellationToken);
            var evt = Serializer.Deserialize(await eventDataTask.ConfigureAwait(false), eventType);

            if (evt is IEventSourceEvent eventSourceEvent)
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