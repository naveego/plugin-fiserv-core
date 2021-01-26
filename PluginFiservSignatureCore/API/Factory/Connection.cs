using System.Data;
using System.Data.Odbc;
using System.Threading.Tasks;
using IBM.Data.DB2.iSeries;
using PluginFiservSignatureCore.Helper;

namespace PluginFiservSignatureCore.API.Factory
{
    public class Connection : IConnection
    {
        private readonly iDB2Connection _conn;
        private readonly Settings _settings;

        public Connection(Settings settings)
        {
             _conn = new iDB2Connection(settings.GetConnectionString());
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