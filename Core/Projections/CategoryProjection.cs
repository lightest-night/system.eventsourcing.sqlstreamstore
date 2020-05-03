using System;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using SqlStreamStore;
using SqlStreamStore.Streams;

namespace LightestNight.System.EventSourcing.SqlStreamStore.Projections
{
    public class CategoryProjection : IEventSourceProjection
    {
        private readonly IStreamStore _streamStore;
        private static readonly ConcurrentDictionary<string, int> Checkpoints = new ConcurrentDictionary<string, int>();

        public CategoryProjection(IStreamStore streamStore)
        {
            _streamStore = streamStore;
        }

        public async Task ProcessEvents(string streamId, NewStreamMessage[] events, CancellationToken cancellationToken = default)
        {
            var categoryName = GetCategoryName(streamId);
            var expectedVersion = Checkpoints.ContainsKey(categoryName)
                ? Checkpoints[categoryName]
                : await _streamStore.GetLastVersionOfStream(categoryName, cancellationToken);
            
            await _streamStore.AppendToStream(categoryName, expectedVersion, events, cancellationToken);
            Checkpoints[categoryName] = expectedVersion + events.Length;
        }

        public static void ClearInternalCache()
        {
            Checkpoints.Clear();
        }

        private static string GetCategoryName(string streamId)
        {
            var categoryName = $"{Constants.CategoryPrefix}{Regex.Replace(streamId, @"[({]?[a-fA-F0-9]{8}[-]?([a-fA-F0-9]{4}[-]?){3}[a-fA-F0-9]{12}[})]?", string.Empty, RegexOptions.IgnoreCase)}";
            return categoryName.Substring(0, categoryName.LastIndexOf("-", StringComparison.Ordinal));
        }
    }
}