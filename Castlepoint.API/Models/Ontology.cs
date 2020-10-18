using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Castlepoint.REST
{
    public class Ontology
    {
        public string OntologyUri { get; set; }
        public string Version { get; set; }
        public string Label { get; set; }
        public string Description { get; set; }
        public string Type { get; set; }
    }
}
