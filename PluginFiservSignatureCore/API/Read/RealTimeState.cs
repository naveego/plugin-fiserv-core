using System;

namespace PluginFiservSignatureCore.API.Read
{
    public class RealTimeState
    {
        public long LastJournalEntryId { get; set; } = 0;
        public long JobVersion { get; set; } = -1;
        public long ShapeVersion { get; set; } = -1;
    }
}