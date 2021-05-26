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
using Naveego.Sdk.Logging;

namespace PluginFiservSignatureCore.API.Read
{
    public static partial class Read
    {
        private static readonly string JournalQuery =
            @"SELECT JOCTRR, JOLIB, JOMBR, JOSEQN, JOENTT FROM {0}.{1} WHERE JOSEQN > {2} AND JOLIB = '{3}' AND JOMBR = '{4}' AND JOCODE = 'R'";

        private static readonly string MaxSeqQuery = @"select MAX(JOSEQN) as MAX_JOSEQN FROM {0}.{1}";

        private static readonly string RrnQuery = @"{0} {1} RRN({2}) = {3}";

        private const string RealTimeRecordsCollectionName = "realtimerecord";

        private const string RealTimeStateCollectionName = "realtimestate";

        private const string TempCollectionName = "temp";

        public static bool useTestQuery = false;
        // private static readonly string rrnTestQuery = @"{0} {1} {2}.{3}.RRN = {4}";

        private static Schema _schema;
        private static uint _jobVersion;
        private static uint _shapeVersion;
        private static string _jobId;
        private static long _recordsCount;
        private static string _path;
        private static IConnection _conn;
        private static RealTimeState _realTimeState;
        private static RealTimeSettings _realTimeSettings;
        private static ReadRequest _request;
        private static DateTime _lastLightSync = DateTime.Now;
        private static ILiteCollection<RealTimeState> _realTimeStateCollection;
        private static ILiteCollection<RealTimeRecord> _realTimeRecordsCollection;
        private static ILiteCollection<RealTimeRecord> _tempCollection;

        public class RealTimeRecord
        {
            [BsonId] public string Id { get; set; }
            [BsonField] public Dictionary<string, object> Data { get; set; }
        }

        public static async Task<long> ReadRecordsRealTimeAsync(IConnectionFactory connectionFactory,
            ReadRequest request,
            RealTimeSettings realTimeSettings,
            IServerStreamWriter<Record> responseStream,
            ServerCallContext context, string permanentPath, bool lightSync, bool singleRead)
        {
            Logger.Info("Beginning to read records real time...");

            _request = request;
            _realTimeSettings = realTimeSettings;
            _schema = request.Schema;
            _jobVersion = request.DataVersions.JobDataVersion;
            _shapeVersion = request.DataVersions.ShapeDataVersion;
            _jobId = request.DataVersions.JobId;
            _recordsCount = 0;
            _conn = connectionFactory.GetConnection();
            await _conn.OpenAsync();

            try
            {
                // setup db directory
                _path = Path.Join(permanentPath, "realtime", _jobId);
                Directory.CreateDirectory(_path);

                using (var db = new LiteDatabase(Path.Join(_path, "RealTimeReadRecords.db")))
                {
                    // realtime state
                    _realTimeStateCollection = db.GetCollection<RealTimeState>(RealTimeStateCollectionName);
                    var realTimeStateId = $"{_jobVersion}_{_shapeVersion}";
                    _realTimeState = _realTimeStateCollection.FindOne(r => r.Id == realTimeStateId) ??
                                     new RealTimeState(_jobVersion, _shapeVersion);

                    _realTimeRecordsCollection = db.GetCollection<RealTimeRecord>(RealTimeRecordsCollectionName);

                    _tempCollection = db.GetCollection<RealTimeRecord>(TempCollectionName);

                    Logger.Info("Real time read initializing...");

                    // check to see if we need to load all the data
                    if (_jobVersion > _realTimeState.JobVersion || _shapeVersion > _realTimeState.ShapeVersion)
                    {
                        await PreLoadCollection(connectionFactory, responseStream, _realTimeRecordsCollection, true,
                            singleRead);
                    }

                    Logger.Info("Real time read initialized.");

                    do
                    {
                        Logger.Debug(
                            $"Getting all records after sequence {JsonConvert.SerializeObject(_realTimeState.LastJournalEntryIdMap, Formatting.Indented)}");

                        await _conn.OpenAsync();

                        // get all changes for each table since last sequence number
                        foreach (var table in _realTimeSettings.TableInformation)
                        {
                            Logger.Debug(
                                $"Getting all records after sequence {table.GetTargetJournalAlias()} {_realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()]}");

                            // get all changes for table since last sequence number
                            var cmd = connectionFactory.GetCommand(string.Format(JournalQuery,
                                table.TargetJournalLibrary,
                                table.TargetJournalName,
                                _realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()],
                                table.TargetTableLibrary, table.TargetTableName), _conn);

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
                                Logger.Debug(
                                    $"Found changes to records after sequence {table.GetTargetJournalAlias()} {_realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()]}");

                                while (await reader.ReadAsync())
                                {
                                    var libraryName = reader.GetValueById("JOLIB", '"').ToString();
                                    var tableName = reader.GetValueById("JOMBR", '"').ToString();
                                    var relativeRecordNumber = reader.GetValueById("JOCTRR", '"').ToString();
                                    var journalSequenceNumber = reader.GetValueById("JOSEQN", '"').ToString();
                                    var deleteFlag = reader.GetValueById("JOENTT", '"').ToString() == "DL";
                                    var recordId = $"{libraryName}_{tableName}_{relativeRecordNumber}";

                                    // update maximum sequence number
                                    if (Convert.ToInt64(journalSequenceNumber) >
                                        _realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()])
                                    {
                                        _realTimeState.LastJournalEntryIdMap[table.GetTargetJournalAlias()] =
                                            Convert.ToInt64(journalSequenceNumber);
                                    }

                                    if (deleteFlag)
                                    {
                                        // delete record
                                        Logger.Info($"Deleting record {recordId}");

                                        // handle record deletion
                                        var realtimeRecord =
                                            _realTimeRecordsCollection.FindOne(r => r.Id == recordId);
                                        if (realtimeRecord == null)
                                        {
                                            continue;
                                        }

                                        _realTimeRecordsCollection.DeleteMany(r =>
                                            r.Id == recordId);

                                        var record = new Record
                                        {
                                            Action = Record.Types.Action.Delete,
                                            DataJson = JsonConvert.SerializeObject(realtimeRecord.Data)
                                        };

                                        await responseStream.WriteAsync(record);
                                        _recordsCount++;
                                    }
                                    else
                                    {
                                        Logger.Info($"Upserting record {recordId}");

                                        var wherePattern = @"\s[^[]?[wW][hH][eE][rR][eE][^]]?\s";
                                        var whereReg = new Regex(wherePattern);
                                        var whereMatch = whereReg.Matches(request.Schema.Query);

                                        ICommand cmdRrn;
                                        if (whereMatch.Count == 1)
                                        {
                                            cmdRrn = connectionFactory.GetCommand(
                                                string.Format(RrnQuery, request.Schema.Query, "AND",
                                                    table.GetTargetTableAlias(),
                                                    relativeRecordNumber),
                                                _conn);
                                        }
                                        else
                                        {
                                            cmdRrn = connectionFactory.GetCommand(
                                                string.Format(RrnQuery, request.Schema.Query, "WHERE",
                                                    table.GetTargetTableAlias(),
                                                    relativeRecordNumber), _conn);
                                        }

                                        // read actual row
                                        try
                                        {
                                            var readerRrn = await cmdRrn.ExecuteReaderAsync();

                                            if (readerRrn.HasRows())
                                            {
                                                while (await readerRrn.ReadAsync())
                                                {
                                                    var recordMap = new Dictionary<string, object>();
                                                    var recordKeysMap = new Dictionary<string, object>();
                                                    foreach (var property in _schema.Properties)
                                                    {
                                                        try
                                                        {
                                                            switch (property.Type)
                                                            {
                                                                case PropertyType.String:
                                                                case PropertyType.Text:
                                                                case PropertyType.Decimal:
                                                                    recordMap[property.Id] =
                                                                        readerRrn.GetValueById(property.Id, '"')
                                                                            .ToString();
                                                                    if (property.IsKey)
                                                                    {
                                                                        recordKeysMap[property.Id] =
                                                                            readerRrn.GetValueById(property.Id, '"')
                                                                                .ToString();
                                                                    }

                                                                    break;
                                                                default:
                                                                    recordMap[property.Id] =
                                                                        readerRrn.GetValueById(property.Id, '"');
                                                                    if (property.IsKey)
                                                                    {
                                                                        recordKeysMap[property.Id] =
                                                                            readerRrn.GetValueById(property.Id, '"');
                                                                    }

                                                                    break;
                                                            }

                                                            // update local db
                                                            var realTimeRecord = new RealTimeRecord
                                                            {
                                                                Id = recordId,
                                                                Data = recordKeysMap
                                                            };

                                                            // upsert record into db
                                                            _realTimeRecordsCollection.Upsert(realTimeRecord);
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            Logger.Error(e,
                                                                $"No column with property Id: {property.Id}");
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
                                                    _recordsCount++;
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            Logger.Error(e, e.Message);
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        // commit state for last run
                        if (!singleRead)
                        {
                            var realTimeStateCommit = new Record
                            {
                                Action = Record.Types.Action.RealTimeStateCommit,
                                RealTimeStateJson = JsonConvert.SerializeObject(_realTimeState)
                            };
                            await responseStream.WriteAsync(realTimeStateCommit);
                        }

                        _realTimeStateCollection.Upsert(_realTimeState);

                        Logger.Info(
                            $"Got all records up to sequence {JsonConvert.SerializeObject(_realTimeState.LastJournalEntryIdMap, Formatting.Indented)}");

                        // light sync deletes
                        if (lightSync && (DateTime.Compare(DateTime.Now, _lastLightSync.AddMinutes(30)) >= 0 ||
                                          singleRead))
                        {
                            _lastLightSync = DateTime.Now;
                            await PreLoadCollection(connectionFactory, responseStream, _tempCollection, false,
                                singleRead);

                            foreach (var realTimeRecord in _realTimeRecordsCollection.FindAll())
                            {
                                if (_tempCollection.FindById(realTimeRecord.Id) == null)
                                {
                                    // delete record
                                    Logger.Info($"Deleting record {realTimeRecord.Id}");

                                    // handle record deletion
                                    _realTimeRecordsCollection.DeleteMany(r =>
                                        r.Id == realTimeRecord.Id);

                                    var record = new Record
                                    {
                                        Action = Record.Types.Action.Delete,
                                        DataJson = JsonConvert.SerializeObject(realTimeRecord.Data)
                                    };

                                    await responseStream.WriteAsync(record);
                                    _recordsCount++;
                                }
                            }
                        }

                        if (singleRead)
                        {
                            break;
                        }

                        await Task.Delay(_realTimeSettings.PollingIntervalSeconds * (1000), context.CancellationToken);
                    } while (!context.CancellationToken.IsCancellationRequested);
                }
            }
            catch (TaskCanceledException e)
            {
                Logger.Info($"Operation cancelled {e.Message}");
                await _conn.CloseAsync();
                return _recordsCount;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message, context);
                throw;
            }
            finally
            {
                await _conn.CloseAsync();
            }

            return _recordsCount;
        }

        private static async Task PreLoadCollection(IConnectionFactory connFactory,
            IServerStreamWriter<Record> responseStream, ILiteCollection<RealTimeRecord> realTimeRecordsCollection,
            bool writeRecords, bool singleRead)
        {
            // realtime state
            var realTimeStateId = $"{_jobVersion}_{_shapeVersion}";
            _realTimeState = _realTimeStateCollection.FindOne(r => r.Id == realTimeStateId) ??
                             new RealTimeState(_jobVersion, _shapeVersion);

            var rrnKeys = new List<string>();
            var rrnSelect = new StringBuilder();
            foreach (var table in _realTimeSettings.TableInformation)
            {
                rrnKeys.Add(table.GetTargetTableName());
                rrnSelect.Append($",RRN({table.GetTargetTableAlias()}) as {table.GetTargetTableName()}");
            }

            // check for UNIONS
            var unionPattern = @"[Uu][Nn][Ii][Oo][Nn]";
            var unionResult = Regex.Split(_request.Schema.Query, unionPattern);
            var loadQuery = new StringBuilder();
            if (unionResult.Length == 0)
            {
                var fromPattern = @"[Ff][Rr][Oo][Mm]";
                var fromResult = Regex.Split(_request.Schema.Query, fromPattern);
                loadQuery.Append($"{fromResult[0]}{rrnSelect}\nFROM {fromResult[1]}");
            }
            else
            {
                var index = 0;
                foreach (var union in unionResult)
                {
                    var fromPattern = @"[Ff][Rr][Oo][Mm]";
                    var fromResult = Regex.Split(union, fromPattern);
                    loadQuery.Append($"{fromResult[0]}{rrnSelect}\nFROM {fromResult[1]}");
                    index++;
                    if (index != unionResult.Length)
                    {
                        loadQuery.Append(" UNION ");
                    }
                }
            }

            // delete existing collection
            realTimeRecordsCollection.DeleteAll();

            if (realTimeRecordsCollection.LongCount() == 0)
            {
                var cmd = connFactory.GetCommand(loadQuery.ToString(), _conn);

                var readerRealTime = await cmd.ExecuteReaderAsync();

                // check for changes to process
                if (readerRealTime.HasRows())
                {
                    while (await readerRealTime.ReadAsync())
                    {
                        // record map to send to response stream
                        var recordMap = new Dictionary<string, object>();
                        var recordKeysMap = new Dictionary<string, object>();
                        foreach (var property in _schema.Properties)
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
                                        if (property.IsKey)
                                        {
                                            recordKeysMap[property.Id] =
                                                readerRealTime.GetValueById(property.Id, '"').ToString();
                                        }

                                        break;
                                    default:
                                        recordMap[property.Id] =
                                            readerRealTime.GetValueById(property.Id, '"');
                                        if (property.IsKey)
                                        {
                                            recordKeysMap[property.Id] =
                                                readerRealTime.GetValueById(property.Id, '"');
                                        }

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

                        // build local db entry
                        foreach (var rrnKey in rrnKeys)
                        {
                            try
                            {
                                var rrn = readerRealTime.GetValueById(rrnKey, '"');

                                // Create new real time record
                                var realTimeRecord = new RealTimeRecord
                                {
                                    Id = $"{rrnKey}_{rrn}",
                                    Data = recordKeysMap
                                };

                                // Insert new record into db
                                realTimeRecordsCollection.Upsert(realTimeRecord);
                            }
                            catch (Exception e)
                            {
                                Logger.Error(e, $"No column with property Id: {rrnKey}");
                                Logger.Error(e, e.Message);
                            }
                        }

                        // Publish record
                        if (writeRecords)
                        {
                            var record = new Record
                            {
                                Action = Record.Types.Action.Upsert,
                                DataJson = JsonConvert.SerializeObject(recordMap)
                            };

                            await responseStream.WriteAsync(record);
                        }

                        _recordsCount++;
                    }
                }

                // get current max sequence numbers
                var maxSeqMap = _realTimeSettings.TableInformation
                    .GroupBy(t => t.GetTargetJournalAlias())
                    .ToDictionary(t => t.Key, x => (long) 0);

                foreach (var seqItem in maxSeqMap)
                {
                    var idSplit = seqItem.Key.Split("_");
                    var seqCmd = connFactory.GetCommand(string.Format(MaxSeqQuery, idSplit[0], idSplit[1]),
                        _conn);

                    var seqReader = await seqCmd.ExecuteReaderAsync();

                    if (seqReader.HasRows())
                    {
                        await seqReader.ReadAsync();
                        _realTimeState.LastJournalEntryIdMap[seqItem.Key] =
                            Convert.ToInt64(seqReader.GetValueById("MAX_JOSEQN"));
                    }
                }
            }


            // commit base real time state
            _realTimeState.JobVersion = _jobVersion;
            _realTimeState.ShapeVersion = _shapeVersion;

            if (!singleRead)
            {
                var realTimeStateCommit = new Record
                {
                    Action = Record.Types.Action.RealTimeStateCommit,
                    RealTimeStateJson = JsonConvert.SerializeObject(_realTimeState)
                };
                await responseStream.WriteAsync(realTimeStateCommit);
            }

            _realTimeStateCollection.Upsert(_realTimeState);

            Logger.Debug($"Got all records for reload");
        }
    }
}