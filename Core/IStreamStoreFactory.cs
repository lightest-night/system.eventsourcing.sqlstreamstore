using System.Threading;
using System.Threading.Tasks;
using SqlStreamStore;

namespace LightestNight.System.EventSourcing.SqlStreamStore
{
    public interface IStreamStoreFactory
    {
        Task<IStreamStore> GetStreamStore(int retries = 3, CancellationToken cancellationToken = default);
    }
}