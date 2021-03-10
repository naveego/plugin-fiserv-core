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
        private static readonly string journalQuery =
            @"SELECT JOCTRR, JOLIB, JOMBR, JOSEQN, JOENTT FROM {0}.{1} WHERE JOSEQN > {2} AND JOLIB = '{3}' AND JOMBR = '{4}'";

        private static readonly string rrnQuery = @"{0} {1} RRN({2}) = {3}";

        public static async Task<long> ReadRecordsRealTimeAsync(IConnectionFactory connFactory, ReadRequest request,
            IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            Logger.Info("Beginning to read records real time...");

            var schema = request.Schema;
            var jobVersion = request.DataVersions.JobDataVersion;
            var shapeVersion = request.DataVersions.ShapeDataVersion;
            var recordsCount = 0;
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            try
            {
                Logger.Info("Real time read initializing...");
                var realTimeSettings = JsonConvert.DeserializeObject<RealTimeSettings>(request.RealTimeSettingsJson);
                var realTimeState = !string.IsNullOrWhiteSpace(request.RealTimeStateJson)
                    ? JsonConvert.DeserializeObject<RealTimeState>(request.RealTimeStateJson)
                    : new RealTimeState();

                // check to see if we need to load all the data
                if (jobVersion > realTimeState.JobVersion || shapeVersion > realTimeState.ShapeVersion)
                {
                    // Read all records for query
                    var records = Read.ReadRecords(connFactory, schema);

                    await foreach (var record in records)
                    {
                        // publish record
                        await responseStream.WriteAsync(record);
                        recordsCount++;
                    }

                    realTimeState.JobVersion = jobVersion;
                    realTimeState.ShapeVersion = shapeVersion;
                }

                Logger.Info("Real time read initialized.");

                while (!context.CancellationToken.IsCancellationRequested)
                {
                    var maxSequenceNumber = realTimeState.LastJournalEntryId;
                    
                    Logger.Debug($"Getting all records after sequence {realTimeState.LastJournalEntryId}");
                    
                    // get all changes for each table since last sequence number
                    foreach (var table in realTimeSettings.TableInformation)
                    {
                        // get all changes for table since last sequence number
                        var cmd = connFactory.GetCommand(string.Format(journalQuery, table.TargetJournalLibrary,
                            table.TargetJournalName, realTimeState.LastJournalEntryId,
                            table.TargetTableLibrary, table.TargetTableName), conn);

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

                        // check for changes to process
                        if (reader.HasRows())
                        {
                            while (await reader.ReadAsync())
                            {
                                var connectionString = reader.GetValueById("JOLIB", '"').ToString();
                                var tableName = reader.GetValueById("JOMBR", '"').ToString();
                                var rowNumber = reader.GetValueById("JOCTRR", '"').ToString();
                                var lastJournalEntryId = reader.GetValueById("JOSEQN", '"').ToString();
                                var deleteFlag = reader.GetValueById("JOENTT", '"').ToString() == "DL";

                                // update maximum sequence number
                                if (Convert.ToInt64(lastJournalEntryId) > maxSequenceNumber)
                                {
                                    maxSequenceNumber = Convert.ToInt64(lastJournalEntryId);
                                }
                                
                                if (deleteFlag)
                                {
                                    // handle record deletion
                                    // TODO: handle deleted records
                                    var record = new Record
                                    {
                                        Action = Record.Types.Action.Delete,
                                    };

                                    await responseStream.WriteAsync(record);
                                    recordsCount++;
                                }
                                else
                                {
                                    // handle reading changed records
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
                                    var cmdRRN = connFactory.GetCommand("", connRRN);
                                    if (whereMatch.Count == 1)
                                    {
                                        cmdRRN = connFactory.GetCommand(
                                            string.Format(rrnQuery, request.Schema.Query, "AND", tableShortName,
                                                rowNumber),
                                            connRRN);
                                    }
                                    else
                                    {
                                        cmdRRN = connFactory.GetCommand(
                                            string.Format(rrnQuery, request.Schema.Query, "WHERE", tableShortName,
                                                rowNumber), connRRN);
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
                                                            recordMap[property.Id] =
                                                                readerRRN.GetValueById(property.Id, '"').ToString();
                                                            break;
                                                        default:
                                                            recordMap[property.Id] =
                                                                readerRRN.GetValueById(property.Id, '"');
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
                                }
                            }
                        }
                    }
                    
                    // commit state for last run
                    realTimeState.JobVersion = jobVersion;
                    realTimeState.ShapeVersion = shapeVersion;
                    realTimeState.LastJournalEntryId = maxSequenceNumber;

                    var realTimeStateCommit = new Record
                    {
                        Action = Record.Types.Action.RealTimeStateCommit,
                        RealTimeStateJson = JsonConvert.SerializeObject(realTimeState)
                    };
                    await responseStream.WriteAsync(realTimeStateCommit);

                    Logger.Debug($"Got all records up to sequence {realTimeState.LastJournalEntryId}");

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