using LightestNight.System.EventSourcing.SqlStreamStore.Serialization.Newtonsoft;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization
{
    public static class SerializerFactory
    {
        public static ISerializer GetSerializer()
            => new Serializer();
    }
}