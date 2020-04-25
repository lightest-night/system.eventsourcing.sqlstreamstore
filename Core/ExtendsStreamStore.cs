using System;
using System.Threading;
using System.Threading.Tasks;
using LightestNight.System.Utilities.Extensions;
using SqlStreamStore;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public static class ExtendsStreamStore
    {
        /// <summary>
        /// Reads the stream from the end and goes back 1 event. This will get us the last version in the stream
        /// </summary>
        /// <param name="streamStore">The instance of <see cref="IStreamStore" /></param>
        /// <param name="streamId">The name of the stream to read</param>
        /// <param name="cancellationToken">Any <see cref="CancellationToken" /> to use to marshall the request</param>
        /// <returns>The last version in the stream</returns>
        public static async Task<int> GetLastVersionOfStream(this IStreamStore streamStore, string streamId, CancellationToken cancellationToken = default)
        {
            var streamResult = await GetLastVersionOfStream<object?>(streamStore, streamId, cancellationToken);
            return streamResult.LastStreamVersion;
        }
        
        /// <summary>
        /// Reads the stream from the end and goes back 1 event. This will get us the last version in the stream
        /// </summary>
        /// <param name="streamStore">The instance of <see cref="IStreamStore" /></param>
        /// <param name="streamId">The name of the stream to read</param>
        /// <param name="cancellationToken">Any <see cref="CancellationToken" /> to use to marshall the request</param>
        /// <typeparam name="T">The type of the object returned by the GetDataFunc</typeparam>
        /// <returns>A tuple containing the last version in the stream and a function to gather the contents of the last event as the given Type</returns>
        public static async Task<(int LastStreamVersion, Func<CancellationToken, Task<T>> GetDataFunc)> GetLastVersionOfStream<T>(this IStreamStore streamStore, string streamId,
            CancellationToken cancellationToken = default)
        {
            var streamResult = await streamStore.ReadStreamBackwards(streamId, StreamVersion.End, 1, cancellationToken: cancellationToken);
            return (streamResult.LastStreamVersion, token => streamResult.Messages.IsNullOrEmpty()
                ? Task.FromResult(default(T)!)
                : streamResult.Messages[0].GetJsonDataAs<T>(token));
        }
    }
}