using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Castlepoint.Utilities
{
    public static class Comparers
    {
        public static Dictionary<string, string> CompareDictionary(Dictionary<string, string> dict1, Dictionary<string,string> dict2)
        {
            Dictionary<string, string> results = new Dictionary<string, string>();

            if (dict1==null || dict2==null)
            {
                return results;
            }

            // Find dict1 keys with different values compared to dict2
            foreach(string k1 in dict1.Keys)
            {
                // Check if this key exists in dict2
                if (!dict2.Keys.Contains(k1) )
                {
                    // Add as a compare difference
                    results.Add(k1, dict1[k1] + "|");
                }
                else
                {
                    // Key exists in both dict1 and dict2
                    // Compare the values
                    if (dict1[k1]!=dict2[k1])
                    {
                        // Values are different
                        results.Add(k1, dict1[k1] + "|" + dict2[k1]);
                    }
                }
            }

            // Find dict2 keys that don't exist in dict1
            foreach(string k2 in dict2.Keys)
            {
                if (!dict1.Keys.Contains(k2))
                    {
                    // Add as a compare value
                    results.Add(k2, "|" + dict2[k2]);
                }
            }

            return results;
        }
    }
}
