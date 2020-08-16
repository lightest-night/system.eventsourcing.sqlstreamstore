using System;
using Newtonsoft.Json;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization.Newtonsoft
{
    public class Serializer : ISerializer
    {
        private readonly JsonSerializerSettings _settings;
        
        public Serializer()
        {
            _settings = new JsonSerializerSettings
            {
                DateParseHandling = DateParseHandling.DateTimeOffset,
                DateFormatHandling = DateFormatHandling.IsoDateFormat,
                DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind,
                TypeNameHandling = TypeNameHandling.Auto,
                Converters = { new ByteArrayConverter() }
            };    
        }

        /// <inheritdoc cref="ISerializer.Serialize" />
        public string Serialize(object? value, Type? type = null)
            => JsonConvert.SerializeObject(value, type, _settings);

        /// <inheritdoc cref="ISerializer.Deserialize{T}" />
        public T Deserialize<T>(string value)
            => JsonConvert.DeserializeObject<T>(value, _settings)!;

        /// <inheritdoc cref="ISerializer.Deserialize" />
        public object? Deserialize(string value, Type type)
            => JsonConvert.DeserializeObject(value, type, _settings);
    }
}