using System;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization
{
    public interface ISerializer
    {
        /// <summary>
        /// Serializes the given value, referencing as the given type
        /// </summary>
        /// <param name="value">The object to serialize</param>
        /// <param name="type">The original <see cref="Type" /> of the object being serialized</param>
        /// <returns>The serialized string representation of the object</returns>
        string Serialize(object? value, Type? type = null);

        /// <summary>
        /// Deserializes the given string
        /// </summary>
        /// <param name="value">The string to deserialize</param>
        /// <typeparam name="T">The type of object being returned</typeparam>
        /// <returns>A deserialized instance of <typeparamref name="T" /></returns>
        T Deserialize<T>(string value);

        /// <summary>
        /// Deserializes the given string
        /// </summary>
        /// <param name="value">The string to deserialize</param>
        /// <param name="type">The type of the object being returned</param>
        /// <returns>A deserialized instance</returns>
        object? Deserialize(string value, Type type);
    }
}