using System;
using LightestNight.System.EventSourcing.Events;
using Newtonsoft.Json;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    [EventType]
    public class TestEvent : EventSourceEvent
    {
        public Guid Id { get; }
        public string? Property { get; }

        [JsonConstructor]
        public TestEvent(Guid id, string? property = null)
        {
            Id = id;
            Property = property;
        }
    }
}