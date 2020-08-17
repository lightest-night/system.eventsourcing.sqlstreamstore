namespace LightestNight.System.EventSourcing.SqlStreamStore.Serialization
{
    public static class SerializerFactory
    {
        public static ISerializer Get { get; private set; } = new Newtonsoft.Serializer();

        public static void SetSerializer(ISerializer serializer)
        {
            Get = serializer;
        }
    }
}