using System;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Checkpoints;
using LightestNight.System.EventSourcing.Events;
using LightestNight.System.EventSourcing.Observers;
using LightestNight.System.EventSourcing.SqlStreamStore.Subscriptions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Shouldly;
using SqlStreamStore;
using SqlStreamStore.Streams;
using Xunit;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Subscriptions
{
    public class EventSubscriptionTests : IAsyncLifetime, IDisposable
    {
        private readonly IStreamStore _streamStore;
        private readonly EventSubscription _sut;
        private readonly IEventObserver _observer;
        
        private object? _observedEvent;
        private readonly ManualResetEventSlim _waitEvent = new ManualResetEventSlim(false);

        public EventSubscriptionTests()
        {
            _streamStore = new InMemoryStreamStore();

            _observer = new TestObserver(true, false, @event =>
            {
                _observedEvent = @event;
                _waitEvent.Set();
            });

            _sut = new EventSubscription(new NullLogger<EventSubscription>(), _streamStore,
                Mock.Of<SetGlobalCheckpoint>(), Mock.Of<GetGlobalCheckpoint>());
        }

        public Task InitializeAsync()
            => ObserverCollection.RegisterObserverAsync(_observer, CancellationToken.None);

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

        public Task DisposeAsync()
            => Task.CompletedTask;
    }
}