using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using PluginODBC.DataContracts;

namespace PluginODBC.API.Write
{
    public static partial class Write
    {
        public static string GetSchemaJson(List<WriteStoredProcedure> storedProcedures)
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    {"StoredProcedure", new Dictionary<string, object>
                    {
                        {"type", "string"},
                        {"title", "Stored Procedure"},
                        {"description", "Stored Procedure to call"},
                        {"enum", storedProcedures.Select(s => s.GetId())}
                    }},
                }},
                {"required", new []
                {
                    "StoredProcedure"
                }}
            };

            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}