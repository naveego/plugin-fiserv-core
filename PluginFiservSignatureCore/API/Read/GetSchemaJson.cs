using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginFiservSignatureCore.API.Read
{
    public static partial class Read
    {
        public static string GetSchemaJson()
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    {"PollingInterval", new Dictionary<string, object>
                    {
                        {"type", "number"},
                        {"title", "Polling Interval"},
                        {"description", "How frequently to poll the api for changes in seconds (default of 5s)"},
                        {"default", 5},
                    }},
                }},
                {"required", new []
                {
                    "PollingInterval"
                }}
            };
            
            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}