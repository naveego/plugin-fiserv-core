using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginFiservSignatureCore.API.Replication
{
    public static partial class Replication
    {
        public static string GetUIJson()
        {
            var uiJsonObj = new Dictionary<string, object>
            {
                {"ui:order", new []
                {
                    "SchemaName",
                    "GoldenTableName",
                    "VersionTableName"
                }}
            };

            return JsonConvert.SerializeObject(uiJsonObj);
        }
    }
}