using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Files.SharePoint2010
{
    public class FileResultJson
    {
        public FileNamespace d { get; set; }
    }
    public class FileNamespace
    {
        public List<FileResult> results { get; set; }

    }
    public class FileResult : POCO.FileBatch
    {
        //public string Author { get; set; }
        public int Length { get; set; }
        //public string ModifiedBy { get; set; }
        public string Name { get; set; }
        public string ServerRelativeUrl { get; set; }
        public string Title { get; set; }
        public string Created { get; set; }
        public string Modified { get; set; }
        public string UIVersion { get; set; }
    }
}
