using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Castlepoint.REST
{
    internal class PagedData
    {
        public object data;
        public int totalRecords;
        public string nextPageId;
    }
}
