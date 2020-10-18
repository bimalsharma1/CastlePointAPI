using System;
using System.Collections.Generic;
using System.Text;

namespace Castlepoint.POCO
{
    
    public class DocumentText
    {
        public DocumentText()
        {
            parts = new List<DocumentPart>();
        }
        public List<DocumentPart> parts { get; set; }

        public int GetLength()
        {
            int length = 0;

            if (this.parts!=null)
            {
                foreach(POCO.DocumentPart part in this.parts)
                {
                    length += part.length;
                }
            }

            return length;
        }
    }

    public class DocumentPart
    {
        public DocumentPart()
        {
            this.partnumber = 0;
            this.header = string.Empty;
            this.body = string.Empty;
            this.footer = string.Empty;
        }
        public int partnumber { get; set; }
        public string header { get; set; }
        public string body { get; set; }
        public string footer { get; set; }
        public int length
        {
            get
            {
                if (body != null)
                {
                    return this.body.Length;
                }
                else
                {
                    return 0;
                }
            }
        }
    }
}
