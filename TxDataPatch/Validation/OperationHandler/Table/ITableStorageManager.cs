using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Durable.Function.TxDataPatch.Validation.OperationHandler.Table
{
    public interface ITableStorageManager
    {
        Task InsertCustomerIntoTableStorage(string blobName, string correlationId, string status, string errorMessages, ILogger log);
    }
}