using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginFiservSignatureCore.API.Factory;
using PluginFiservSignatureCore.Helper;
using LiteDB;

namespace PluginFiservSignatureCore.API.Read
{
    public static partial class Read
    {
        private static readonly string JournalQuery =
            @"SELECT JOCTRR, JOLIB, JOMBR, JOSEQN, JOENTT FROM {0}.{1} WHERE JOSEQN > {2} AND JOLIB = '{3}' AND JOMBR = '{4}' AND JOCODE = 'R'";

        private static readonly string RrnQuery = @"{0} {1} RRN({2}) = {3}";

        private const string CollectionName = "realtimerecord";
        
        public static bool useTestQuery = false;
        private static readonly string rrnTestQuery = @"{0} {1}.RRN = {3}";

        public class RealTimeRecord
        {
            public string Id { get; set; }
            public Dictionary<string, object> Data { get; set; }
        }

        public static async Task<long> ReadRecordsRealTimeAsync(IConnectionFactory connFactory, ReadRequest request,
            IServerStreamWriter<Record> responseStream,
            ServerCallContext context)
        {
            Logger.Info("Beginning to read records real time...");

            var schema = request.Schema;
            var schemaKeys = new List<Property>();
            schemaKeys.AddRange(schema.Properties);
            schemaKeys = schemaKeys.Where(p => p.IsKey).ToList();
            var jobVersion = request.DataVersions.JobDataVersion;
            var shapeVersion = request.DataVersions.ShapeDataVersion;
            var jobId = request.DataVersions.JobId;
            var recordsCount = 0;
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            try
            {
                // setup db directory
                var path = $"db/{jobId}";
                Directory.CreateDirectory(path);

                using (var db = new LiteDatabase($"{path}/RealTimeReadRecords.db"))
                {
                    var realtimeRecordsCollection = db.GetCollection<RealTimeRecord>(CollectionName);
                    Logger.Info("Real time read initializing...");


                    var realTimeSettings =
                        JsonConvert.DeserializeObject<RealTimeSettings>(request.RealTimeSettingsJson);
                    var realTimeState = !string.IsNullOrWhiteSpace(request.RealTimeStateJson)
                        ? JsonConvert.DeserializeObject<RealTimeState>(request.RealTimeStateJson)
                        : new RealTimeState();

                    // check to see if we need to load all the data
                    if (jobVersion > realTimeState.JobVersion || shapeVersion > realTimeState.ShapeVersion)
                    {
                        var rrnMap = new Dictionary<string, string>();
                        var rrnSelect = new StringBuilder();
                        foreach (var table in realTimeSettings.TableInformation)
                        {
                            rrnMap.Add(table.GetTargetTableAlias(), "");
                            rrnSelect.Append($",\nRRN({table.GetTargetTableName()}) as {table.GetTargetTableAlias()}");
                        }

                        // check for UNIONS
                        string unionPattern = @"[Uu][Nn][Ii][Oo][Nn]";
                        string[] unionResult = Regex.Split(request.Schema.Query, unionPattern);
                        var loadQuery = new StringBuilder();
                        if (unionResult.Length == 0)
                        {
                            string fromPattern = @"[Ff][Rr][Oo][Mm]";
                            string[] fromResult = Regex.Split(request.Schema.Query, fromPattern);
                            loadQuery.Append($"{fromResult[0]}{rrnSelect} FROM {fromResult[1]}");
                        }
                        else
                        {
                            int index = 0;
                            foreach (var union in unionResult)
                            {
                                string fromPattern = @"[Ff][Rr][Oo][Mm]";
                                string[] fromResult = Regex.Split(union, fromPattern);
                                loadQuery.Append($"{fromResult[0]}{rrnSelect} FROM {fromResult[1]}");
                                index++;
                                if (index != unionResult.Length)
                                {
                                    loadQuery.Append(" UNION ");
                                }
                            }
                        }

                        // delete existing collection
                        realtimeRecordsCollection.DeleteAll();

                        var cmd = connFactory.GetCommand(loadQuery.ToString(), conn);

                        var readerRealTime = await cmd.ExecuteReaderAsync();

                        long maxRrn = 0;
                        // check for changes to process
                        if (readerRealTime.HasRows())
                        {
                            while (await readerRealTime.ReadAsync())
                            {
                                var recordMap = new Dictionary<string, object>();
                                foreach (var property in schemaKeys)
                                {
                                    try
                                    {
                                        switch (property.Type)
                                        {
                                            case PropertyType.String:
                                            case PropertyType.Text:
                                            case PropertyType.Decimal:
                                                recordMap[property.Id] =
                                                    readerRealTime.GetValueById(property.Id, '"').ToString();
                                                break;
                                            default:
                                                recordMap[property.Id] =
                                                    readerRealTime.GetValueById(property.Id, '"');
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

                                foreach (var rrnKey in rrnMap.Keys)
                                {
                                    try
                                    {
                                        var rrn = readerRealTime.GetValueById(rrnKey, '"');
                                        if (Convert.ToInt64(rrn) > maxRrn)
                                        {
                                            maxRrn = Convert.ToInt64(rrn);
                                        }

                                        // Create your new customer instance
                                        var realTimeRecord = new RealTimeRecord
                                        {
                                            Id = $"{rrn}_{rrnKey}",
                                            Data = recordMap
                                        };

                                        // Insert new record into db
                                        realtimeRecordsCollection.Insert(realTimeRecord);
                                    }
                                    catch (Exception e)
                                    {
                                        Logger.Error(e, $"No column with property Id: {rrnKey}");
                                        Logger.Error(e, e.Message);
                                        rrnMap[rrnKey] = null;
                                    }
                                }

                                // Publish record
                                var record = new Record
                                {
                                    Action = Record.Types.Action.Upsert,
                                    DataJson = JsonConvert.SerializeObject(recordMap)
                                };

                                await responseStream.WriteAsync(record);
                                recordsCount++;
                            }
                        }

                        realTimeState.LastJournalEntryId = maxRrn;
                        realTimeState.JobVersion = jobVersion;
                        realTimeState.ShapeVersion = shapeVersion;

                        var realTimeStateCommit = new Record
                        {
                            Action = Record.Types.Action.RealTimeStateCommit,
                            RealTimeStateJson = JsonConvert.SerializeObject(realTimeState)
                        };
                        await responseStream.WriteAsync(realTimeStateCommit);

                        Logger.Debug($"Got all records up to sequence {realTimeState.LastJournalEntryId}");
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
                            var cmd = connFactory.GetCommand(string.Format(JournalQuery, table.TargetJournalLibrary,
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
                                    var libraryName = reader.GetValueById("JOLIB", '"').ToString();
                                    var tableName = reader.GetValueById("JOMBR", '"').ToString();
                                    var relativeRecordNumber = reader.GetValueById("JOCTRR", '"').ToString();
                                    var journalSequenceNumber = reader.GetValueById("JOSEQN", '"').ToString();
                                    var deleteFlag = reader.GetValueById("JOENTT", '"').ToString() == "DL";

                                    // update maximum sequence number
                                    if (Convert.ToInt64(journalSequenceNumber) > maxSequenceNumber)
                                    {
                                        maxSequenceNumber = Convert.ToInt64(journalSequenceNumber);
                                    }

                                    if (deleteFlag)
                                    {
                                        // handle record deletion
                                        var realtimeRecord = realtimeRecordsCollection.FindOne(r =>
                                            r.Id == $"{relativeRecordNumber}_{libraryName}_{tableName}");
                                        if (realtimeRecord == null)
                                        {
                                            continue;
                                        }

                                        realtimeRecordsCollection.DeleteMany(r => r.Id == $"{relativeRecordNumber}_{libraryName}_{tableName}");

                                        var record = new Record
                                        {
                                            Action = Record.Types.Action.Delete,
                                            DataJson = JsonConvert.SerializeObject(realtimeRecord.Data)
                                        };

                                        await responseStream.WriteAsync(record);
                                        recordsCount++;
                                    }
                                    else
                                    {
                                        // handle reading changed records
                                        string tablePattern = libraryName + "." + tableName + @"\s[a-zA-Z0-9]*";
                                        Regex tableReg = new Regex(tablePattern);
                                        MatchCollection tableMatch = tableReg.Matches(request.Schema.Query);
                                        var tableShortNameArray = tableMatch[0].Value.Split(' ');
                                        var tableShortName = tableShortNameArray[1];

                                        string wherePattern = @"\s[wW][hH][eE][rR][eE]\s[a-zA-Z0-9.\s=><'""]*\Z";
                                        Regex whereReg = new Regex(wherePattern);
                                        MatchCollection whereMatch = whereReg.Matches(request.Schema.Query);

                                        var connRrn = connFactory.GetConnection();
                                        await connRrn.OpenAsync();
                                        
                                        ICommand cmdRrn;
                                        if (whereMatch.Count == 1)
                                        {
                                            cmdRrn = connFactory.GetCommand(
                                                string.Format(RrnQuery, request.Schema.Query, "AND", tableShortName,
                                                    relativeRecordNumber),
                                                connRrn);
                                        }
                                        else
                                        {
                                            cmdRrn = connFactory.GetCommand(
                                                string.Format(RrnQuery, request.Schema.Query, "WHERE", tableShortName,
                                                    relativeRecordNumber), connRrn);
                                        }

                                        // read actual row
                                        IReader readerRrn;
                                        try
                                        {
                                            readerRrn = await cmdRrn.ExecuteReaderAsync();

                                            if (readerRrn.HasRows())
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
                                                                    readerRrn.GetValueById(property.Id, '"').ToString();
                                                                break;
                                                            default:
                                                                recordMap[property.Id] =
                                                                    readerRrn.GetValueById(property.Id, '"');
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
                                            await connRrn.CloseAsync();
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