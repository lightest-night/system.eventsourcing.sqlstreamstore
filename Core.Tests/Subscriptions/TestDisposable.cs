using System;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Core.Tests.Subscriptions
{
    public class TestDisposable : IDisposable
    {
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        protected virtual void Dispose(bool disposing){}
    }
}