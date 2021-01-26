using System.Data;
using System.Threading.Tasks;
using System.Data.Odbc;
using PluginODBC.Helper;

namespace PluginODBC.API.Factory
{
    public class Connection : IConnection
    {
        private readonly OdbcConnection _conn;
        private readonly Settings _settings;

        public Connection(Settings settings)
        {
             _conn = new OdbcConnection(settings.GetConnectionString());
            _settings = settings;
        }

        public async Task OpenAsync()
        {
            await _conn.OpenAsync();
        }

        public async Task CloseAsync()
        {
            await _conn.CloseAsync();
        }

        public Task<bool> PingAsync()
        {
            return Task.FromResult(true);
        }

        public IDbConnection GetConnection()
        {
            return _conn;
        }
    }
}