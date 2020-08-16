using System;
using System.Text.Json;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization.SystemTextJson
{
    public class Serializer : ISerializer
    {
        private readonly JsonSerializerOptions _options;
        
        public Serializer()
        {
            _options = new JsonSerializerOptions
            {
                IgnoreNullValues = true
            };
        }
        
        /// <inheritdoc cref="ISerializer.Serialize" />
        public string Serialize(object? value, Type? type = null)
            => JsonSerializer.Serialize(value, type, _options);

        /// <inheritdoc cref="ISerializer.Deserialize{T}" />
        public T Deserialize<T>(string value)
            => JsonSerializer.Deserialize<T>(value, _options);

        /// <inheritdoc cref="ISerializer.Deserialize" />
        public object? Deserialize(string value, Type type)
            => JsonSerializer.Deserialize(value, type, _options);
    }
}