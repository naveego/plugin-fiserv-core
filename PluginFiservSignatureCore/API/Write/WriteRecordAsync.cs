using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginFiservSignatureCore.API.Factory;
using PluginFiservSignatureCore.Helper;

namespace PluginFiservSignatureCore.API.Write
{
    public static partial class Write
    {
        private static readonly SemaphoreSlim WriteSemaphoreSlim = new SemaphoreSlim(1, 1);

        public static async Task<string> WriteRecordAsync(IConnectionFactory connFactory, Schema schema, Record record,
            IServerStreamWriter<RecordAck> responseStream)
        {
            // debug
            Logger.Debug($"Starting timer for {record.RecordId}");
            var timer = Stopwatch.StartNew();
            
            var conn = connFactory.GetConnection();

            try
            {
                var recordMap = JsonConvert.DeserializeObject<Dictionary<string, object>>(record.DataJson);

                // debug
                Logger.Debug(JsonConvert.SerializeObject(record, Formatting.Indented));

                // semaphore
                await WriteSemaphoreSlim.WaitAsync();

                // call stored procedure
                var querySb = new StringBuilder($"CALL {schema.Query}(");

                foreach (var property in schema.Properties)
                {
                    if (!recordMap.ContainsKey(property.Id))
                    {
                        throw new Exception($"{property.Id} is required by the stored procedure and is not mapped on the job.");
                    }

                    var rawValue = recordMap[property.Id];
                    
                    if (rawValue == null || string.IsNullOrWhiteSpace(rawValue.ToString()))
                    {
                        querySb.Append("NULL,");
                    }
                    else
                    {
                        querySb.Append($"'{Utility.Utility.GetSafeString(Utility.Utility.GetSafeString(rawValue.ToString(), "'", "''"))}',");
                    }
                }

                querySb.Length--;
                querySb.Append(")");

                var query = querySb.ToString();
                
                Logger.Debug($"WB querySb: {query}");
                
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(query, conn);

                await cmd.ExecuteNonQueryAsync();

                await conn.CloseAsync();

                var ack = new RecordAck
                {
                    CorrelationId = record.CorrelationId,
                    Error = ""
                };
                await responseStream.WriteAsync(ack);

                timer.Stop();
                Logger.Debug($"Acknowledged Record {record.RecordId} time: {timer.ElapsedMilliseconds}");

                return "";
            }
            catch (Exception e)
            {
                await conn.CloseAsync();
                
                Logger.Error(e, $"Error writing record {e.Message}");
                // send ack
                var ack = new RecordAck
                {
                    CorrelationId = record.CorrelationId,
                    Error = e.Message
                };
                await responseStream.WriteAsync(ack);

                timer.Stop();
                Logger.Debug($"Failed Record {record.RecordId} time: {timer.ElapsedMilliseconds}");

                return e.Message;
            }
            finally
            {
                WriteSemaphoreSlim.Release();
            }
        }
    }
}