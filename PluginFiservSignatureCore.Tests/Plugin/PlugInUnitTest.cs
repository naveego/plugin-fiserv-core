using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Moq;
using Naveego.Sdk.Plugins;
using PluginFiservSignatureCore.Helper;
using PluginFiservSignatureCore.API.Factory;
using PluginFiservSignatureCore.API.Read;

using Xunit;
using Newtonsoft.Json;
using Record = Naveego.Sdk.Plugins.Record;

namespace PluginFiservSignatureCore.Plugin
{
    public class PluginTest
    {
        private readonly Mock<IConnection> _mockOdbcConnection = new Mock<IConnection>();

        private ConnectRequest GetConnectSettings()
        {
            return new ConnectRequest
            {
                SettingsJson =
                    "{\"ConnectionString\":\"test connection\",\"Password\":\"password\",\"PrePublishQuery\":\"\",\"PostPublishQuery\":\"\",\"_server\":{\"Connected\":true}}",
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            };
        }

        private Dictionary<string,object> SendCancellationRequest()
        {
            Task.Delay(10000);
            Dictionary<string, object> context  = new Dictionary<string, object>(); 
            Dictionary<string, bool> cancellationToken  = new Dictionary<string, bool>(); 
            cancellationToken.Add("IsCancellationRequested", true);
            context.Add("CancellationToken", cancellationToken);
            return context;
        }
        
        private IConnectionFactory GetMockConnectionFactory()
        {
          var mockFactory = new Mock<IConnectionFactory>();

          mockFactory.Setup(m => m.GetConnection())
            .Returns(_mockOdbcConnection.Object);

          mockFactory.Setup(m => m.GetConnection().PingAsync())
            .Returns(Task.FromResult(true));

          mockFactory.Setup(m => m.GetCommand("DiscoverSchemas", _mockOdbcConnection.Object))
            .Returns(() =>
            {
                var mockOdbcCommand = new Mock<ICommand>();

                mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                    .Returns(() =>
                    {
                        var mockReader = new Mock<IReader>();

                        mockReader.Setup(r => r.GetSchemaTable())
                            .Returns(() =>
                            {
                                var mockSchemaTable = new DataTable();
                                mockSchemaTable.Columns.AddRange(new[]
                                    {
                                        new DataColumn
                                        {
                                            ColumnName = "ColumnName"
                                        },
                                        new DataColumn
                                        {
                                            ColumnName = "DataType"
                                        },
                                        new DataColumn
                                        {
                                            ColumnName = "IsKey"
                                        },
                                        new DataColumn
                                        {
                                            ColumnName = "AllowDBNull"
                                        },
                                    }
                                );

                                var mockRow = mockSchemaTable.NewRow();
                                mockRow["ColumnName"] = "TestCol";
                                mockRow["DataType"] = "System.Int64";
                                mockRow["IsKey"] = true;
                                mockRow["AllowDBNull"] = false;

                                mockSchemaTable.Rows.Add(mockRow);


                                return mockSchemaTable;
                            });

                        return Task.FromResult(mockReader.Object);
                    });

                return mockOdbcCommand.Object;
            });


            mockFactory.Setup(m => m.GetCommand($"SELECT COUNT(*) as count FROM ({"DiscoverSchemas"}) as q", _mockOdbcConnection.Object))
              .Returns(() =>
              {
                  var mockOdbcCommand = new Mock<ICommand>();

                  mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                      .Returns(() =>
                      {
                          var mockReader = new Mock<IReader>();

                          mockReader.Setup(r => r.HasRows())
                              .Returns(true);

                          var readToggle = new List<bool> {true, true, false};
                          var readIndex = 0;
                          mockReader.Setup(r => r.ReadAsync())
                              .Returns(() => Task.FromResult(readToggle[readIndex]))
                              .Callback(() => readIndex++);

                          mockReader.Setup(r => r.GetValueById("count", '"'))
                              .Returns(1);

                          return Task.FromResult(mockReader.Object);
                      });

                  return mockOdbcCommand.Object;
              });

            mockFactory.Setup(m => m.GetCommand("ReadStream", _mockOdbcConnection.Object))
              .Returns(() =>
              {
                  var mockOdbcCommand = new Mock<ICommand>();

                  mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                      .Returns(() =>
                      {
                          var mockReader = new Mock<IReader>();

                          mockReader.Setup(r => r.HasRows())
                              .Returns(true);

                          var readToggle = new List<bool> {true, true, false};
                          var readIndex = 0;
                          mockReader.Setup(r => r.ReadAsync())
                              .Returns(() => Task.FromResult(readToggle[readIndex]))
                              .Callback(() => readIndex++);

                          mockReader.Setup(r => r.GetValueById("TestCol", '"'))
                              .Returns("data");

                          return Task.FromResult(mockReader.Object);
                      });

                  return mockOdbcCommand.Object;
              });
            
            mockFactory.Setup(m => m.GetCommand("SELECT JOCTRR, JOLIB, JOMBR, JOSEQN FROM HLDQRY001.KPCBT11101 WHERE JOSEQN > 0 AND JOLIB = BNKPRD01 AND JOMBR = TAP00201", _mockOdbcConnection.Object))
                .Returns(() =>
                {
                    var mockOdbcCommand = new Mock<ICommand>();

                    mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                        .Returns(() =>
                        {
                            var mockReader = new Mock<IReader>();

                            mockReader.Setup(r => r.HasRows())
                                .Returns(true);

                            var readToggle = new List<bool> {true, true, false};
                            var readIndex = 0;

                            var resultRows = new List<Dictionary<string,object>>
                            {
                                {new Dictionary<string, object>{
                                    {"JOLIB", "BNKPRD01"},
                                    {"JOMBR", "TAP00201"},
                                    {"JOCTRR", "734837"},
                                    {"JOSEQN","1310675271"}
                                }},
                                {new Dictionary<string, object>{
                                    {"JOLIB", "BNKPRD01"},
                                    {"JOMBR", "TAP00201"},
                                    {"JOCTRR", "734838"},
                                    {"JOSEQN","1310675272"}
                                }},
                            };

                            mockReader.Setup(r => r.ReadAsync())
                                .Returns(() => Task.FromResult(readToggle[readIndex]))
                                .Callback(() => readIndex++);

                            mockReader.Setup(r => r.GetValueById("JOLIB", '"'))
                                .Returns(resultRows[readIndex]["JOLIB"]);
                            mockReader.Setup(r => r.GetValueById("JOMBR", '"'))
                                .Returns(resultRows[readIndex]["JOMBR"]);
                            mockReader.Setup(r => r.GetValueById("JOCTRR", '"'))
                                .Returns(resultRows[readIndex]["JOCTRR"]);
                            mockReader.Setup(r => r.GetValueById("JOSEQN", '"'))
                                .Returns(resultRows[readIndex]["JOSEQN"]);                          

                            return Task.FromResult(mockReader.Object);
                        });

                    return mockOdbcCommand.Object;
                });

                mockFactory.Setup(m => m.GetCommand("SELECT JOCTRR, JOLIB, JOMBR, JOSEQN FROM HLDQRY001.KPCBT11101 WHERE JOSEQN > 1310675271 AND JOLIB = BNKPRD01 AND JOMBR = TAP00201", _mockOdbcConnection.Object))
                .Returns(() =>
                {
                    var mockOdbcCommand = new Mock<ICommand>();

                    mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                        .Returns(() =>
                        {
                            var mockReader = new Mock<IReader>();

                            mockReader.Setup(r => r.HasRows())
                                .Returns(true);

                            var readToggle = new List<bool> {false};
                            var readIndex = 0;

                            mockReader.Setup(r => r.ReadAsync())
                                .Returns(() => Task.FromResult(readToggle[readIndex]))
                                .Callback(() => readIndex++);                        

                            return Task.FromResult(mockReader.Object);
                        });

                    return mockOdbcCommand.Object;
                });

                mockFactory.Setup(m => m.GetCommand("SELECT a.1\n                b.2\n                c.2\n                FROM BNKPRD01.TAP00201 a\n                LEFT OUTER JOIN BNKPRD01.TAP00202 b\n                ON a.1 = b.1\n                LEFT OUTER JOIN BNKPRD01.TAP002 c\n                ON a.1 = c.1 WHERE RRN(a) = 734837", _mockOdbcConnection.Object))
                .Returns(() =>
                {
                    var mockOdbcCommand = new Mock<ICommand>();

                    mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                        .Returns(() =>
                        {
                            var mockReader = new Mock<IReader>();

                            mockReader.Setup(r => r.HasRows())
                                .Returns(true);

                            var readToggle = new List<bool> {true, false};
                            var readIndex = 0;
                            mockReader.Setup(r => r.ReadAsync())
                                .Returns(() => Task.FromResult(readToggle[readIndex]))
                                .Callback(() => readIndex++);

                            mockReader.Setup(r => r.GetValueById("Id", '"'))
                                .Returns(5);
                            mockReader.Setup(r => r.GetValueById("Name", '"'))
                                .Returns("Test");                   

                            return Task.FromResult(mockReader.Object);
                        });

                    return mockOdbcCommand.Object;
                });
                
                
                // real time read mocks
                mockFactory.Setup(m => m.GetCommand(@"SELECT
DISTINCT CFBRCH
,RRN(BNKPRD95.CFP10201) as BNKPRD95_CFP10201
FROM  BNKPRD95.CFP10201", _mockOdbcConnection.Object))
                .Returns(() =>
                {
                    var mockOdbcCommand = new Mock<ICommand>();

                    mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                        .Returns(() =>
                        {
                            var mockReader = new Mock<IReader>();

                            mockReader.Setup(r => r.HasRows())
                                .Returns(true);
                            
                            mockReader.SetupSequence(r => r.ReadAsync())
                                .Returns(() => Task.FromResult(true))
                                .Returns(() => Task.FromResult(true))
                                .Returns(() => Task.FromResult(false));


                            mockReader.SetupSequence(r => r.GetValueById("CFBRCH", '"'))
                                .Returns("13")
                                .Returns("13")
                                .Returns("1")
                                .Returns("1");
                            
                            mockReader.SetupSequence(r => r.GetValueById("BNKPRD95_CFP10201", '"'))
                                .Returns("1")
                                .Returns("2");  

                            return Task.FromResult(mockReader.Object);
                        });

                    return mockOdbcCommand.Object;
                });
                
                mockFactory.Setup(m => m.GetCommand(@"select MAX(JOSEQN) as MAX_JOSEQN FROM BNKPRD95.CBT11101", _mockOdbcConnection.Object))
                    .Returns(() =>
                    {
                        var mockOdbcCommand = new Mock<ICommand>();

                        mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                            .Returns(() =>
                            {
                                var mockReader = new Mock<IReader>();

                                mockReader.Setup(r => r.HasRows())
                                    .Returns(true);
                                
                                mockReader.SetupSequence(r => r.ReadAsync())
                                    .Returns(() => Task.FromResult(true))
                                    .Returns(() => Task.FromResult(false));

                                mockReader.Setup(r => r.GetValueById("MAX_JOSEQN", '"'))
                                    .Returns("1000");
                                
                                return Task.FromResult(mockReader.Object);
                            });

                        return mockOdbcCommand.Object;
                    });
                
                mockFactory.Setup(m => m.GetCommand(@"SELECT JOCTRR, JOLIB, JOMBR, JOSEQN, JOENTT FROM BNKPRD95.CBT11101 WHERE JOSEQN > 1000 AND JOLIB = 'BNKPRD95' AND JOMBR = 'CFP10201' AND JOCODE = 'R'", _mockOdbcConnection.Object))
                    .Returns(() =>
                    {
                        var mockOdbcCommand = new Mock<ICommand>();

                        mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                            .Returns(() =>
                            {
                                var mockReader = new Mock<IReader>();

                                mockReader.Setup(r => r.HasRows())
                                    .Returns(true);
                            
                                mockReader.SetupSequence(r => r.ReadAsync())
                                    .Returns(() => Task.FromResult(true))
                                    .Returns(() => Task.FromResult(true))
                                    .Returns(() => Task.FromResult(false));


                                mockReader.SetupSequence(r => r.GetValueById("JOLIB", '"'))
                                    .Returns("BNKPRD95")
                                    .Returns("BNKPRD95");
                            
                                mockReader.SetupSequence(r => r.GetValueById("JOMBR", '"'))
                                    .Returns("CFP10201")
                                    .Returns("CFP10201");
                                
                                mockReader.SetupSequence(r => r.GetValueById("JOCTRR", '"'))
                                    .Returns("1")
                                    .Returns("2");
                                
                                mockReader.SetupSequence(r => r.GetValueById("JOSEQN", '"'))
                                    .Returns("1001")
                                    .Returns("1002");
                                
                                mockReader.SetupSequence(r => r.GetValueById("JOENTT", '"'))
                                    .Returns("UP")
                                    .Returns("DL");

                                return Task.FromResult(mockReader.Object);
                            });

                        return mockOdbcCommand.Object;
                    });
                
                mockFactory.Setup(m => m.GetCommand(@"SELECT
DISTINCT CFBRCH
FROM BNKPRD95.CFP10201 WHERE RRN(BNKPRD95.CFP10201) = 1", _mockOdbcConnection.Object))
                    .Returns(() =>
                    {
                        var mockOdbcCommand = new Mock<ICommand>();

                        mockOdbcCommand.Setup(c => c.ExecuteReaderAsync())
                            .Returns(() =>
                            {
                                var mockReader = new Mock<IReader>();

                                mockReader.Setup(r => r.HasRows())
                                    .Returns(true);
                            
                                mockReader.SetupSequence(r => r.ReadAsync())
                                    .Returns(() => Task.FromResult(true))
                                    .Returns(() => Task.FromResult(false));


                                mockReader.SetupSequence(r => r.GetValueById("CFBRCH", '"'))
                                    .Returns("13");

                                return Task.FromResult(mockReader.Object);
                            });

                        return mockOdbcCommand.Object;
                    });

            return mockFactory.Object;
        }

        private Schema GetTestSchema(string query)
        {
            return new Schema
            {
                Id = "test",
                Name = "test",
                Query = query
            };
        }

        private Schema GetTestSchemaRealTime()
        {
            return new Schema
            {
                Id = "test",
                Name = "test",
                Query = @"SELECT
DISTINCT CFBRCH
FROM BNKPRD95.CFP10201",
                Properties =    
                {
                    new Property
                    {
                        Id = "CFBRCH",
                        Name = "CFBRCH",
                        Type = PropertyType.String,
                        IsKey = true
                    },
                }
            };
        }

        private RealTimeSettings GetRealTimeSettings()
        {
            return new RealTimeSettings
            {
                PollingIntervalSeconds = 5,
                TableInformation = new List<RealTimeSettings.JournalInfo>
                    {
                        new RealTimeSettings.JournalInfo
                        {
                            TargetJournalLibrary= "BNKPRD95",
                            TargetJournalName= "CBT11101",
                            TargetTableLibrary = "BNKPRD95",
                            TargetTableName ="CFP10201"
                        }
                    }
            };
        }

        private List<Record> GetTestRecords()
        {
            return new List<Record>
            {
                new Record
                {
                    CorrelationId = "test",
                    DataJson = "{\"TestCol\":\"\"}"
                },
                new Record
                {
                    CorrelationId = "more-test",
                    DataJson = "{\"TestCol\":\"\"}"
                }
            };
        }


        [Fact]
        public async Task ConnectSessionTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();
            var disconnectRequest = new DisconnectRequest();

            // act
            var response = client.ConnectSession(request);
            var responseStream = response.ResponseStream;
            var records = new List<ConnectResponse>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
                client.Disconnect(disconnectRequest);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasAllTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.All,
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Empty(response.Schemas);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {GetTestSchema("DiscoverSchemas")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal("test", schema.Id);
            Assert.Equal("test", schema.Name);
            Assert.Single(schema.Properties);

            var property = schema.Properties[0];
            Assert.Equal("\"TestCol\"", property.Id);
            Assert.Equal("TestCol", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.Integer, property.Type);
            Assert.True(property.IsKey);
            Assert.False(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new ReadRequest()
            {
                Schema = GetTestSchema("ReadStream"),
                Limit = 2
            };

            // act
            client.Connect(connectRequest);
            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(2, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamLimitTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new ReadRequest()
            {
                Schema = GetTestSchema("ReadStream"),
                Limit = 1
            };

            // act
            client.Connect(connectRequest);
            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamRealTimeTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);
            
            var configureRequest = new ConfigureRequest
            {
                TemporaryDirectory = "../../../Temp",
                PermanentDirectory = "../../../Perm",
                LogDirectory = "../../../Logs",
                DataVersions = new DataVersions(),
                LogLevel = LogLevel.Debug
            };

            var connectRequest = GetConnectSettings();

            var request = new ReadRequest()
            {
                Schema = GetTestSchemaRealTime(),
                Limit = 2,
                RealTimeSettingsJson = JsonConvert.SerializeObject(GetRealTimeSettings()),
                RealTimeStateJson = JsonConvert.SerializeObject(new RealTimeState(1,1)),
                DataVersions = new DataVersions
                {
                    JobId = "test-job",
                    JobDataVersion = 1,
                    ShapeId = "test-shape",
                    ShapeDataVersion = 1
                },
            };
            
            // act
            // number of records w/ w/out data, number of real time state commits, real time state last read
            var records = new List<Record>();
            try
            {
                client.Configure(configureRequest);
                client.Connect(connectRequest);


                var cancellationToken = new CancellationTokenSource();
                cancellationToken.CancelAfter(5000);
                var response = client.ReadStream(request, null, null, cancellationToken.Token);
                var responseStream = response.ResponseStream;


                while (await responseStream.MoveNext())
                {
                    records.Add(responseStream.Current);
                }
            }
            catch (Exception e)
            {
                Assert.Contains("Status(StatusCode=\"Cancelled\", Detail=\"Cancelled\",", e.Message);
            }
            
            // assert
            var recordsRts = records.Where(r => r.Action == Record.Types.Action.RealTimeStateCommit).ToList();
            Assert.Equal(6, records.Count); // total
            Assert.Equal(2, recordsRts.Count);
            
            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
        
        [Fact]
        public async Task PrepareWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new PrepareWriteRequest
            {
                Schema = GetTestSchema("WriteStream"),
                CommitSlaSeconds = 1
            };

            // act
            client.Connect(connectRequest);
            var response = client.PrepareWrite(request);

            // assert
            Assert.IsType<PrepareWriteResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task WriteStreamTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var schema = GetTestSchema("WriteStream");
            schema.Properties.Add(new Property
            {
                Id = "TestCol",
                Name = "TestCol",
                Type = PropertyType.Integer
            });

            var prepareRequest = new PrepareWriteRequest()
            {
                Schema = schema,
                CommitSlaSeconds = 1
            };

            var records = GetTestRecords();

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);
            client.PrepareWrite(prepareRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Equal(2, recordAcks.Count);
            Assert.Equal("", recordAcks[0].Error);
            Assert.Equal("test", recordAcks[0].CorrelationId);
            Assert.Equal("", recordAcks[1].Error);
            Assert.Equal("more-test", recordAcks[1].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConfigureWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginFiservSignatureCore.Plugin.Plugin(GetMockConnectionFactory()))},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var firstRequest = new ConfigureWriteRequest()
            {
                Form = new ConfigurationFormRequest
                {
                    DataJson = "",
                    StateJson = ""
                }
            };

            var secondRequest = new ConfigureWriteRequest()
            {
                Form = new ConfigurationFormRequest
                {
                    DataJson =
                        "{\"Query\":\"ConfigureWrite\",\"Parameters\":[{\"ParamName\":\"Name\",\"ParamType\":\"int\"}]}",
                    StateJson = ""
                }
            };

            // act
            client.Connect(connectRequest);
            var firstResponse = client.ConfigureWrite(firstRequest);
            var secondResponse = client.ConfigureWrite(secondRequest);

            // assert
            Assert.IsType<ConfigureWriteResponse>(firstResponse);
            Assert.NotNull(firstResponse.Form.SchemaJson);
            Assert.NotNull(firstResponse.Form.UiJson);
            Assert.Null(firstResponse.Schema);

            Assert.IsType<ConfigureWriteResponse>(secondResponse);
            Assert.NotNull(secondResponse.Form.SchemaJson);
            Assert.NotNull(secondResponse.Form.UiJson);
            Assert.NotNull(secondResponse.Schema);
            Assert.Equal("", secondResponse.Schema.Id);
            Assert.Equal("", secondResponse.Schema.Name);
            Assert.Equal("ConfigureWrite", secondResponse.Schema.Query);
            Assert.Equal(Schema.Types.DataFlowDirection.Write, secondResponse.Schema.DataFlowDirection);
            Assert.Single(secondResponse.Schema.Properties);

            var property = secondResponse.Schema.Properties[0];
            Assert.Equal("Name", property.Id);
            Assert.Equal("Name", property.Name);
            Assert.Equal(PropertyType.Integer, property.Type);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
    }
}