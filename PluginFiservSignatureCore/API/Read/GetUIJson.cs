using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginFiservSignatureCore.API.Read
{
    public static partial class Read
    {
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {
                    "ui:order", new[]
                    {
                        "PollingInterval",
                        "TableInformation"
                    }
                },
                {
                    "TableInformation", new[]
                    {
                        "TargetJournalLibrary",
                        "TargetJournalName",
                        "TargetTableLibrary",
                        "TargetTableName"
                    }
                }
            };

            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}