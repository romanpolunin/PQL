using Pql.Engine.DataContainer.Engine;
using Pql.Engine.Interfaces.Services;

namespace Pql.Engine.UnitTest
{
    public class TestableEngineCache : IDataEngineCache
    {
        public DataEngine Engine;

        public TestableEngineCache(DataEngine engine)
        {
            Engine = engine;
        }

        public int GetTotalEnginesCount()
        {
            return 1;
        }

        public IDataEngine GetEngine(string tenantId, string scopeId)
        {
            return Engine;
        }

        public void Dispose()
        {
            
        }
    }
}