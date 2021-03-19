using System.Collections.Generic;

namespace PluginFiservSignatureCore.API.Read
{
    public class RealTimeSettings
    {
        public int PollingIntervalSeconds { get; set; } = 5;
        public List<JournalInfo> TableInformation {get; set; } = new List<JournalInfo>();

        public class JournalInfo {
            public string TargetJournalLibrary {get; set;} = "";
            public string TargetJournalName {get; set;} = "";
            public string TargetTableLibrary {get; set;} = "";
            public string TargetTableName {get; set;} = "";

            public string GetTargetTableAlias()
            {
                return $"{TargetTableLibrary}_{TargetTableName}";
            }
            
            public string GetTargetTableName()
            {
                return $"{TargetTableLibrary}.{TargetTableName}";
            }
        };
    }
}