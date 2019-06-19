using System;

namespace LoggerLib
{
    public class LogDetail
    {
        public string Message { get; set; }
        public int MessageOrder { get; set; }
        public int PartitionNo { get; set; }
        public long OffSet { get; set; }
        public int ConsumerId { get; set; }
    }
}
