namespace Pql.SqlEngine.Interfaces.Services
{
    public interface IDataEngineCache : IDisposable
    {
        int GetTotalEnginesCount();

        IDataEngine GetEngine(string tenantId, string scopeId);
    }
}