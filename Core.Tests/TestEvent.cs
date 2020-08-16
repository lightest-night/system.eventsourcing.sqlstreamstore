using System;
using LightestNight.System.EventSourcing.Events;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    [EventType]
    public class TestEvent : EventSourceEvent
    {
        public Guid Id { get; }
        public string? Property { get; }
        
        public TestObject? Obj { get; }
        
        public TestEvent(Guid id, string? property = null, TestObject? obj = null)
        {
            Id = id;
            Property = property;
            Obj = obj;
        }
    }

    [EventType]
    public class SecondaryTestEvent : EventSourceEvent
    {
        public string? Property { get; }
        
        public TestObject? Obj { get; }
        
        public SecondaryTestEvent(string? property = null, TestObject? obj = null)
        {
            Property = property;
            Obj = obj;
        }
    }

    public class TestObject
    {
        public string Name { get; set; } = default!;
        public int Age { get; set; }
    }
}