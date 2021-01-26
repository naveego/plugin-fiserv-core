using System.Data;
using System.Data.Odbc;
using System.Threading.Tasks;
using IBM.Data.DB2.Core;

namespace PluginFiservSignatureCore.API.Factory
{
    public class Reader : IReader
    {
        private readonly DB2DataReader _reader;

        public Reader(IDataReader reader)
        {
            _reader = (DB2DataReader) reader;
        }

        public async Task<bool> ReadAsync()
        {
            return await _reader.ReadAsync();
        }

        public async Task CloseAsync()
        {
            await _reader.CloseAsync();
        }

        public DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }

        public object GetValueById(string id, char trimChar = '"')
        {
            return _reader[id.Trim(trimChar)];
        }

        public bool HasRows()
        {
            return _reader.HasRows;
        }
    }
}