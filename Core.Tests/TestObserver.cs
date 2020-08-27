using System;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.EventSourcing.Events;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests
{
    public class TestObserver : IEventObserver
    {
        private readonly Action<object> _outcome;
        
        public TestObserver(bool isActive, Action<object> outcome)
        {
            IsActive = isActive;
            _outcome = outcome;
        }

        public Task InitialiseObserver(CancellationToken cancellationToken = new CancellationToken())
            => Task.CompletedTask;

        public Task EventReceived(object evt, long? position = null, int? version = null, CancellationToken cancellationToken = default)
        {
            _outcome(evt);
            return Task.CompletedTask;
        }

        public bool IsActive { get; }
    }
}