using System.Threading;
using System.Threading.Tasks;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Projections
{
    public interface IEventSourceProjection
    {
        /// <summary>
        /// Processes events as they come through the event store into the projection implementation
        /// </summary>
        /// <param name="streamId">The identifier of the stream</param>
        /// <param name="events">The collection of events</param>
        /// <param name="cancellationToken">Any <see cref="CancellationToken" /> necessary to marshal the operation</param>
        Task ProcessEvents(string streamId, NewStreamMessage[] events, CancellationToken cancellationToken = default);
    }
}