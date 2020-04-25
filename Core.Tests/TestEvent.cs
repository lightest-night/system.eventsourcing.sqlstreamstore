using System;
using LightestNight.System.EventSourcing.Events;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    [EventType(nameof(TestEvent))]
    public class TestEvent : IEventSourceEvent
    {
        public Guid Id { get; set; }
    }
}