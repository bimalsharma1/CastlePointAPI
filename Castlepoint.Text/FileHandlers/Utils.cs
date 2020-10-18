using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.XPath;
using System.Xml.Linq;
using System.IO.Packaging;
using System.IO.Compression;

namespace Castlepoint.Text.FileHandlers
{
    internal static class Utils
    {
        internal static System.IO.MemoryStream FixInvalidUri(System.IO.MemoryStream zipStream)
        {
            XNamespace relNs = "http://schemas.openxmlformats.org/package/2006/relationships";
            using (ZipArchive za = new ZipArchive(zipStream, ZipArchiveMode.Update))
            {
                foreach (var entry in za.Entries.ToList())
                {
                    if (!entry.Name.EndsWith(".rels"))
                        continue;
                    bool replaceEntry = false;
                    XDocument entryXDoc = null;
                    using (var entryStream = entry.Open())
                    {
                        try
                        {
                            entryXDoc = XDocument.Load(entryStream);
                            if (entryXDoc.Root != null && entryXDoc.Root.Name.Namespace == relNs)
                            {
                                var urisToCheck = entryXDoc
                                    .Descendants(relNs + "Relationship")
                                    .Where(r => r.Attribute("TargetMode") != null && (string)r.Attribute("TargetMode") == "External");
                                foreach (var rel in urisToCheck)
                                {
                                    var target = (string)rel.Attribute("Target");
                                    if (target != null)
                                    {
                                        try
                                        {
                                            Uri uri = new Uri(target);
                                        }
                                        catch (UriFormatException)
                                        {
                                            Uri newUri = FixUri(target);
                                            rel.Attribute("Target").Value = newUri.ToString();
                                            replaceEntry = true;
                                        }
                                    }
                                }
                            }
                        }
                        catch (XmlException)
                        {
                            continue;
                        }
                    }
                    if (replaceEntry)
                    {
                        var fullName = entry.FullName;
                        entry.Delete();
                        var newEntry = za.CreateEntry(fullName);
                        using (System.IO.StreamWriter writer = new System.IO.StreamWriter(newEntry.Open()))
                        using (XmlWriter xmlWriter = XmlWriter.Create(writer))
                        {
                            entryXDoc.WriteTo(xmlWriter);
                        }
                    }
                }


            }

            return zipStream;
        }

        private static Uri FixUri(string brokenUri)
        {
            return new Uri("http://broken-link/");
        }
    }
}
