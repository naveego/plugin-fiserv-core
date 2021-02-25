using System.Collections.Generic;

namespace PluginFiservSignatureCore.API.Read
{
    public class RealTimeSettings
    {
        public int PollingIntervalSeconds { get; set; } = 5;
        public List<JournalInfo> TableInformation {get; set; } = new List<JournalInfo>();

        public class JournalInfo {
            public string ConnectionName {get; set;} = "";
            public string JournalName {get; set;} = "";
            public string TargetJournalLibrary {get; set;} = "";
            public string TargetTable {get; set;} = "";
        };
    }
}