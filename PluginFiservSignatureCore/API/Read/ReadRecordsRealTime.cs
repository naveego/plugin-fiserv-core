using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginFiservSignatureCore.API.Factory;
using PluginFiservSignatureCore.Helper;

namespace PluginFiservSignatureCore.API.Read
{

    public static partial class Read
    {
        private static readonly string journalQuery = @"SELECT JOCTRR, JOLIB, JOMBR, JOSEQN FROM {0}.{1} WHERE JOSEQN > {2} AND JOLIB = {3} AND JOMBR = {4}";

        private static readonly string rrnQuery = @"{0} {1} RRN({2}) = {3}";

        public static async Task<long> ReadRecordsRealTimeAsync(IConnectionFactory connFactory, ReadRequest request,
            IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            Logger.Info("Beginning to read records real time...");
            
            var schema = request.Schema;
            var jobVersion = request.DataVersions.JobDataVersion;
            var recordsCount = 0;
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            try
            {
                Logger.Info("Real time read initializing...");
                var realTimeSettings = JsonConvert.DeserializeObject<RealTimeSettings>(request.RealTimeSettingsJson);
                var realTimeState = !string.IsNullOrWhiteSpace(request.RealTimeStateJson) ?
                    JsonConvert.DeserializeObject<RealTimeState>(request.RealTimeStateJson) :
                    new RealTimeState();
                var tcs = new TaskCompletionSource<DateTime>();

                if (jobVersion > realTimeState.JobVersion)
                {
                    realTimeState.LastReadTime = DateTime.MinValue;
                }
                
                Logger.Info("Real time read initialized.");

                while (!context.CancellationToken.IsCancellationRequested)
                {
                    foreach( var table in realTimeSettings.TableInformation){
                        var cmd = connFactory.GetCommand(string.Format(journalQuery,table.ConnectionName,table.JournalName, realTimeState.LastJournalEntryId, 
                        table.TargetJournalLibrary, table.TargetTable), conn);

                        long currentRunRecordsCount = 0;
                        Logger.Debug($"Getting all records since {realTimeState.LastReadTime.ToUniversalTime():O}");
                        
                        
                        IReader reader;
                        try
                        {
                            reader = await cmd.ExecuteReaderAsync();
                        }
                        catch (Exception e)
                        {
                            Logger.Error(e, e.Message);
                            break;
                        }
                        
                        if (reader.HasRows())
                        {
                            while (await reader.ReadAsync())
                            {
                                var connectionString = reader.GetValueById("JOLIB", '"').ToString();
                                var tableName = reader.GetValueById("JOMBR", '"').ToString();
                                var rowNumber = reader.GetValueById("JOCTRR", '"').ToString();
                                var LastJournalEntryId = reader.GetValueById("JOSEQN", '"').ToString();
                                
                                string tablePattern = connectionString + "." + tableName + @"\s[a-zA-Z0-9]*";
                                Regex tableReg = new Regex(tablePattern);
                                MatchCollection tableMatch = tableReg.Matches(request.Schema.Query);
                                var tableShortNameArray = tableMatch[0].Value.Split(' ');
                                var tableShortName = tableShortNameArray[1];

                                string wherePattern = @"\s[wW][hH][eE][rR][eE]\s[a-zA-Z0-9.\s=><'""]*\Z";
                                Regex whereReg = new Regex(wherePattern); 
                                MatchCollection whereMatch = whereReg.Matches(request.Schema.Query);
                                
                                var connRRN = connFactory.GetConnection();
                                await connRRN.OpenAsync();
                                var cmdRRN = connFactory.GetCommand("",connRRN);
                                if (whereMatch.Count == 1){
                                    cmdRRN = connFactory.GetCommand(string.Format(rrnQuery,request.Schema.Query,"AND",tableShortName,rowNumber), connRRN); 
                                }
                                else{
                                    cmdRRN = connFactory.GetCommand(string.Format(rrnQuery,request.Schema.Query,"WHERE",tableShortName,rowNumber), connRRN); 
                                }
                                
                                
                                // read actual row
                                IReader readerRRN;
                                try
                                {
                                    readerRRN = await cmdRRN.ExecuteReaderAsync();

                                    if (readerRRN.HasRows()) 
                                    {
                                        var recordMap = new Dictionary<string, object>();

                                        foreach (var property in schema.Properties)
                                        {
                                            try
                                            {
                                                switch (property.Type)
                                                {
                                                    case PropertyType.String:
                                                    case PropertyType.Text:
                                                    case PropertyType.Decimal:
                                                        recordMap[property.Id] = readerRRN.GetValueById(property.Id, '"').ToString();
                                                        break;
                                                    default:
                                                        recordMap[property.Id] = readerRRN.GetValueById(property.Id, '"');
                                                        break;
                                                }
                                            }
                                            catch (Exception e)
                                            {
                                                Logger.Error(e, $"No column with property Id: {property.Id}");
                                                Logger.Error(e, e.Message);
                                                recordMap[property.Id] = null;
                                            }
                                        }

                                        var record = new Record
                                        {
                                            Action = Record.Types.Action.Upsert,
                                            DataJson = JsonConvert.SerializeObject(recordMap)
                                        };

                                        await responseStream.WriteAsync(record);
                                        currentRunRecordsCount++;
                                        recordsCount++;
                                    }
                                }
                                catch (Exception e)
                                {
                                    Logger.Error(e, e.Message);
                                    break;
                                }
                                finally 
                                {
                                    await connRRN.CloseAsync();
                                }

                                realTimeState.LastJournalEntryId = Convert.ToInt64(LastJournalEntryId);

                                if (currentRunRecordsCount%1000 == 0) {
                                    realTimeState.LastReadTime = DateTime.Now;
                                    realTimeState.JobVersion = jobVersion;
                                    
                                    var realTimeStateCommit = new Record
                                    {
                                        Action = Record.Types.Action.RealTimeStateCommit,
                                        RealTimeStateJson = JsonConvert.SerializeObject(realTimeState)
                                    };
                                    await responseStream.WriteAsync(realTimeStateCommit);

                                    Logger.Debug($"Got {currentRunRecordsCount} records since {realTimeState.LastReadTime.ToUniversalTime():O}");
                                }
                            }
                        }
                    }
                    realTimeState.LastReadTime = DateTime.Now;
                    realTimeState.JobVersion = jobVersion;
                    
                    var realTimeStateCommitFinal = new Record
                    {
                        Action = Record.Types.Action.RealTimeStateCommit,
                        RealTimeStateJson = JsonConvert.SerializeObject(realTimeState)
                    };
                    await responseStream.WriteAsync(realTimeStateCommitFinal);

                    Logger.Debug($"Got {recordsCount} records since {realTimeState.LastReadTime.ToUniversalTime():O}");

                    await Task.Delay(realTimeSettings.PollingIntervalSeconds * (1000), context.CancellationToken);
                }
            }
            catch (TaskCanceledException e)
            {
                Logger.Info($"Operation cancelled {e.Message}");
                await conn.CloseAsync();
                return recordsCount;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                throw;
            }
            finally 
            {
                await conn.CloseAsync();
            }
            
            return recordsCount;
        }
    }
}