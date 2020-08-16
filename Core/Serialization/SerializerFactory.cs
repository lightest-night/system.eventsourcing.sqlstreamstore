using System;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization
{
    public static class SerializerFactory
    {
        private static Serializers _serializerInUse = Serializers.Newtonsoft;

        public static void SetSerializerToUse(Serializers serializer)
        {
            if (serializer == Serializers.Unknown)
                serializer = Serializers.Newtonsoft;

            _serializerInUse = serializer;
        }

        public static ISerializer GetSerializer()
            => _serializerInUse switch
            {
                Serializers.Unknown => new Newtonsoft.Serializer(),
                Serializers.Newtonsoft => new Newtonsoft.Serializer(),
                Serializers.Utf8Json => new Utf8Json.Serializer(),
                Serializers.SystemTextJson => new SystemTextJson.Serializer(),
                _ => throw new ArgumentOutOfRangeException($"Serializer {_serializerInUse.ToString()} not recognised")
            };
    }
}