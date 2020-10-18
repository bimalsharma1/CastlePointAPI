using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    public class DataPaging
    {

        public string[] columnFilters { get; set; }
        public string sortType { get; set; }
        public string sortField { get; set; }
        public int page { get; set; }
        public int perPage { get; set; }
        public string thisPageId { get; set; }
        public string matchType { get; set; }

    }
}
