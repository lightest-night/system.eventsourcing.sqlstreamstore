﻿using System;
using System.Collections.Generic;
using LightestNight.System.EventSourcing.Domain;
using LightestNight.System.EventSourcing.Events;

// ReSharper disable SuggestBaseTypeForParameter

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class TestAggregate : EventSourceAggregate
    {
        public TestAggregate(IEnumerable<EventSourceEvent> events) : base(events){}

        public TestAggregate()
        {
            Publish(new TestEvent(Guid.NewGuid()));
        }

        public void SecondaryEvent()
        {
            Publish(new SecondaryTestEvent("Holla!"));
        }
        
        private void When(TestEvent e)
        {
            Id = e.Id;
        }
    }
}