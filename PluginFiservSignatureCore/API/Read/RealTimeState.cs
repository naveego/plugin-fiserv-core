using System;
using System.Collections.Generic;
using LiteDB;

namespace PluginFiservSignatureCore.API.Read
{
    public class RealTimeState
    {
        public RealTimeState(uint jobVersion, uint shapeVersion)
        {
            Id = $"{jobVersion}_{shapeVersion}";
        }
        
        [BsonId] 
        public string Id { get; set; }
        
        [BsonField]
        public Dictionary<string, long> LastJournalEntryIdMap { get; set; } = new Dictionary<string, long>();
        
        [BsonField]
        public long JobVersion { get; set; } = -1;
        
        [BsonField]
        public long ShapeVersion { get; set; } = -1;
    }
}