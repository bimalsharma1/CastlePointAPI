using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO.Connections
{
    public class NTFSConnectionConfig:BaseConnection
    {
        public string NTFSSharePath { get; set; }
        public string Domain { get; set; }
        public string UserId { get; set; }
        public string Password { get; set; }
        public string MountPath { get; set; }
        public string MountPathSeparator { get; set; }
        public string NTFSSharePathSeparator { get; set; }
    }
}
