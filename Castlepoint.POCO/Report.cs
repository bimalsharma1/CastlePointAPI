using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class ReportDefinition
    {
        public string Name { get; set; }
        public string ExportFolder { get; set; }
        public string ExportFilenameFormat { get; set; }
        public string OutputFormat { get; set; }
        public string ExportTemplate { get; set; }
        /// <summary>
        /// Flag to set if the reports are exported to a single file or multiple files
        /// </summary>
        public bool SingleFileOnly { get; set; }
    }
}
