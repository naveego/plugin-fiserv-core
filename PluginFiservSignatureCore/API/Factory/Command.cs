using System.Data.Odbc;
using System.Threading.Tasks;
using IBM.Data.DB2.iSeries;

namespace PluginFiservSignatureCore.API.Factory
{
    public class Command : ICommand
    {
        private readonly iDB2Command _cmd;

        public Command()
        {
            _cmd = new iDB2Command();
        }

        public Command(string commandText)
        {
            _cmd = new iDB2Command(commandText);
        }

        public Command(string commandText, IConnection conn)
        {
            _cmd = new iDB2Command(commandText, (iDB2Connection) conn.GetConnection());
        }

        public void SetConnection(IConnection conn)
        {
            _cmd.Connection = (iDB2Connection) conn.GetConnection();
        }

        public void SetCommandText(string commandText)
        {
            _cmd.CommandText = commandText;
        }

        public void AddParameter(string name, object value)
        {
            _cmd.Parameters.Add(name, value);
        }

        public async Task<IReader> ExecuteReaderAsync()
        {
            return new Reader(await _cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            return await _cmd.ExecuteNonQueryAsync();
        }
    }
}