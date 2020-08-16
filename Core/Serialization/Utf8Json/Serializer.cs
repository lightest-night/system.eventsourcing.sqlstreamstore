using System;
using Utf8Json;
using Utf8Json.Resolvers;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization.Utf8Json
{
    public class Serializer : ISerializer
    {
        private readonly IJsonFormatterResolver _resolver;

        public Serializer()
        {
            _resolver = StandardResolver.AllowPrivateExcludeNullCamelCase;
        }

        /// <inheritdoc cref="ISerializer.Serialize" />
        public string Serialize(object? value, Type? type = null)
            => JsonSerializer.ToJsonString(value, _resolver);

        /// <inheritdoc cref="ISerializer.Deserialize{T}" />
        public T Deserialize<T>(string value)
            => JsonSerializer.Deserialize<T>(value, _resolver);

        /// <inheritdoc cref="ISerializer.Deserialize" />
        public object? Deserialize(string value, Type type)
            => JsonSerializer.NonGeneric.Deserialize(type, value, _resolver);
    }
}