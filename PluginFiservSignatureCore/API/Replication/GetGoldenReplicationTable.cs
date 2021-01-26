using Naveego.Sdk.Plugins;
using PluginFiservSignatureCore.API.Utility;
using PluginFiservSignatureCore.DataContracts;

namespace PluginFiservSignatureCore.API.Replication
{
    public static partial class Replication
    {
        public static ReplicationTable GetGoldenReplicationTable(Schema schema, string safeSchemaName, string safeGoldenTableName)
        {
            var goldenTable = ConvertSchemaToReplicationTable(schema, safeSchemaName, safeGoldenTableName);
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationRecordId,
                DataType = "varchar(255)",
                PrimaryKey = true
            });
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationVersionIds,
                DataType = "clob",
                PrimaryKey = false,
                Serialize = true
            });

            return goldenTable;
        }
    }
}