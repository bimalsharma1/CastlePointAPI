using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.IO;

namespace Castlepoint.POCO
{
    internal static class Utils
    {
        internal static readonly DateTime AzureTableMinDateTime = DateTime.Parse("1601-01-01T00:00:00+00:00").ToUniversalTime();
        internal const string ISODateFormatNoTime = "yyyy-MM-dd";
        internal const string ISODateFormat = "yyyy-MM-ddTHH:mm:ssZ";

        internal static string CleanTableKey(string keyToClean)
        {

            /*
            The following characters are not allowed in PartitionKey and RowKey fields:
                The forward slash (/) character
                The backslash (\) character
                The number sign (#) character 
                The question mark (?) character
            */

            string cleanKey = "";

            // unescape html and url encoding (just in case)
            cleanKey = HttpUtility.HtmlDecode(keyToClean);
            cleanKey = HttpUtility.UrlDecode(cleanKey);

            string patternCarriageReturn = @"\r";
            Regex regCarriageReturn = new Regex(patternCarriageReturn);
            string patternLineFeed = @"\n";
            Regex regLineFeed = new Regex(patternLineFeed);
            string patternTab = @"\t";
            Regex regTab = new Regex(patternTab);

            // Remove carriage returns and line feeds and tabs
            cleanKey = regCarriageReturn.Replace(cleanKey.ToLower(), "");
            cleanKey = regLineFeed.Replace(cleanKey.ToLower(), "");
            cleanKey = regTab.Replace(cleanKey.ToLower(), "");

            string patternForwardSlash = @"\\";
            Regex regForwardSlash = new Regex(patternForwardSlash);
            string patternBackSlash = "/";
            Regex regBackSlash = new Regex(patternBackSlash);
            string patternHash = "#";
            Regex regHash = new Regex(patternHash);
            string patternQuestionMark = @"\?";
            Regex regQuestionMark = new Regex(patternQuestionMark);

            cleanKey = regForwardSlash.Replace(cleanKey.ToLower(), "|");
            cleanKey = regBackSlash.Replace(cleanKey, "|");
            cleanKey = regHash.Replace(cleanKey, "|");
            cleanKey = regQuestionMark.Replace(cleanKey, "|");

            return cleanKey;
        }
    }


}
