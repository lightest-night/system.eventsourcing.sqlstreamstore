using System;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization
{
    public static class SerializerFactory
    {
        private static Serializers _serializerTypeInUse = Serializers.Newtonsoft;
        public static ISerializer Get { get; private set; } = new Newtonsoft.Serializer();

        public static void SetSerializerToUse(Serializers serializer)
        {
            if (serializer == Serializers.Unknown)
                serializer = Serializers.Newtonsoft;

            _serializerTypeInUse = serializer;
            Get = _serializerTypeInUse switch
            {
                Serializers.Unknown => new Newtonsoft.Serializer(),
                Serializers.Newtonsoft => new Newtonsoft.Serializer(),
                Serializers.Utf8Json => new Utf8Json.Serializer(),
                Serializers.SystemTextJson => new SystemTextJson.Serializer(),
                _ => throw new ArgumentOutOfRangeException($"Serializer {_serializerTypeInUse.ToString()} not recognised")
            };
        }
    }
}