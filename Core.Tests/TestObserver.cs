using System;
using System.ComponentModel;
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
        
        public event PropertyChangedEventHandler? PropertyChanged;

        public Task EventReceived(object evt, long? position = null, int? version = null, CancellationToken cancellationToken = default)
        {
            _outcome(evt);
            return Task.CompletedTask;
        }

        private bool _isActive;

        public bool IsActive
        {
            get => _isActive;
            set
            {
                if (value == _isActive)
                    return;

                _isActive = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsActive)));
            }
        }
    }
}