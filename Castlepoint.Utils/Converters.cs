using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Numerics;

namespace Castlepoint.Utilities
{
    public class Converters
    {
        public static string CleanTableKey(string keyToClean)
        {

            /*
            The following characters are not allowed in PartitionKey and RowKey fields:
                The forward slash (/) character
                The backslash (\) character
                The number sign (#) character 
                The question mark (?) character
            */

            // throw error if key is null
            if (keyToClean == null)
            {
                throw new ApplicationException("TableKey is null");
            }

            string cleanKey = "";

            // unescape html and url encoding (just in case)
            cleanKey = System.Web.HttpUtility.HtmlDecode(keyToClean);
            cleanKey = System.Web.HttpUtility.UrlDecode(cleanKey);

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

            // Explicitly convert to lower case
            cleanKey = cleanKey.ToLower();

            // Replace characters with pipe |
            cleanKey = regForwardSlash.Replace(cleanKey, "|");
            cleanKey = regBackSlash.Replace(cleanKey, "|");
            cleanKey = regHash.Replace(cleanKey, "|");
            cleanKey = regQuestionMark.Replace(cleanKey, "|");

            return cleanKey;
        }

        public static string ToBase36String(long toConvert, bool bigEndian = false)
        {
            const string alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            //if (bigEndian) Array.Reverse(toConvert); // !BitConverter.IsLittleEndian might be an alternative
            BigInteger dividend = new BigInteger(toConvert);
            var builder = new StringBuilder();
            while (dividend != 0)
            {
                BigInteger remainder;
                dividend = BigInteger.DivRem(dividend, 36, out remainder);
                builder.Insert(0, alphabet[Math.Abs(((int)remainder))]);
            }
            return builder.ToString();
        }

        public static string StripBookendSlashes(string fileRelativePath)
        {
            // Validate input
            if (fileRelativePath == null || fileRelativePath == string.Empty)
            {
                return string.Empty;
            }

            string returnString = fileRelativePath;

            if (fileRelativePath.StartsWith("/") && fileRelativePath.Length > 1)
            {
                returnString = returnString.Substring(1);
            }

            if (fileRelativePath.EndsWith("/") && fileRelativePath.Length > 1)
            {
                returnString = returnString.Substring(0, returnString.Length - 1);
            }

            return returnString;
        }
    }
}
