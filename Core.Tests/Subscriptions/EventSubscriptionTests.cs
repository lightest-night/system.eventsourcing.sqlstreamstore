using System;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Checkpoints;
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
        private readonly ManualResetEventSlim _waitEvent = new ManualResetEventSlim(false);

        public EventSubscriptionTests()
        {
            _streamStore = new InMemoryStreamStore();

            var observer = new TestObserver(true, @event =>
            {
                _observedEvent = @event;
                _waitEvent.Set();
            });

            _sut = new EventSubscription(new NullLogger<EventSubscription>(), _streamStore, new []{observer},
                Mock.Of<SetGlobalCheckpoint>(), Mock.Of<GetGlobalCheckpoint>());
        }

        [Fact]
        public async Task ShouldDeliverCorrectEvent()
        {
            // Arrange
            var evt = new TestEvent(Guid.NewGuid(),"Test Property");
            await _sut.StartAsync(CancellationToken.None).ConfigureAwait(false);
            
            // Act
            await _streamStore.AppendToStream(Guid.NewGuid().ToString(), ExpectedVersion.NoStream, new[]
            {
                evt.ToMessageData()
            }, CancellationToken.None);
            
            // Give the subscription event time to fire
            _waitEvent.Wait();

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
            _waitEvent.Dispose();
        }
    }
}