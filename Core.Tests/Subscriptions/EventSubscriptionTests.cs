using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Checkpoints;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.SqlStreamStore.Subscriptions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Subscriptions
{
    public class EventSubscriptionTests : IDisposable
    {
        private readonly IStreamStore _streamStore;
        private readonly EventSubscription _sut;
        
        private object? _observedEvent;

        public EventSubscriptionTests()
        {
            _streamStore = new InMemoryStreamStore();

            var observers = new IEventObserver[] {new TestObserver(true, evt => _observedEvent = evt)};

            _sut = new EventSubscription(new NullLogger<EventSubscription>(), _streamStore, observers,
                () => new[] {typeof(TestEvent)}, Mock.Of<SetGlobalCheckpoint>(), Mock.Of<GetGlobalCheckpoint>());
        }

        [Fact]
        public async Task ShouldDeliverCorrectEvent()
        {
            // Arrange
            var evt = new TestEvent(Guid.NewGuid(),"Test Property");
            var headers = new Dictionary<string, object>
            {
                {EventSourcing.Constants.VersionKey, 0}
            };
            await _sut.StartAsync(CancellationToken.None).ConfigureAwait(false);
            
            // Act
            await _streamStore.AppendToStream(Guid.NewGuid().ToString(), ExpectedVersion.NoStream, new[]
                {
                    new NewStreamMessage(
                        Guid.NewGuid(),
                        nameof(TestEvent),
                        JsonSerializer.Serialize(evt, evt.GetType()),
                        JsonSerializer.Serialize(headers))
                },
                CancellationToken.None).ConfigureAwait(false);
            
            // Give the subscription event time to fire
            await Task.Delay(50).ConfigureAwait(false);

            // Assert
            var observedEvent = _observedEvent as TestEvent;
            observedEvent.ShouldNotBeNull();
            observedEvent?.Id.ShouldBe(evt.Id);
            observedEvent?.Property.ShouldBe(evt.Property);
        }

        public void Dispose()
        {
            _streamStore.Dispose();
            _sut.Dispose();
        }
    }
}