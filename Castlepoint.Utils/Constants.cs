using System;

namespace Castlepoint.Utilities
{
    public class Constants
    {
        public const string TableSuffixDateFormatYMD = "yyyyMMdd";
        public const string TableSuffixDateFormatYM = "yyyyMM";
        public const string ISODateFormatNoTime = "yyyy-MM-dd";
        public const string ISODateFormat = "yyyy-MM-ddTHH:mm:ssZ";
        public const string DateFormatConcat = "yyyyMMddTHHmmss";

        public static readonly DateTime AzureTableMinDateTime = DateTime.Parse("1601-01-01T00:00:00+00:00").ToUniversalTime();
    }
}
