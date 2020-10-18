using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using DocumentFormat.OpenXml.Presentation;
using A = DocumentFormat.OpenXml.Drawing;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml;

using System.Text;

using Microsoft.AspNetCore.Http;

namespace Castlepoint.Text
{
    

    public static class PowerPoint
    {
        public static POCO.DocumentText ExtractText(IFormFile upload)
        {
            POCO.DocumentText text = new POCO.DocumentText();

            // Open the presentation as read-only.
            using (PresentationDocument presentationDocument = PresentationDocument.Open(upload.OpenReadStream(), false))
            {
                // Check for a null document object.
                if (presentationDocument == null)
                {
                    throw new ArgumentNullException("presentationDocument");
                }

                int numSlides = CountSlides(presentationDocument);

                for (int i=0;i<numSlides;i++)
                {
                    // Get the text for the slide
                    string slideText = GetSlideIdAndText(presentationDocument, i);

                    // Create a new OfficePart object
                    POCO.DocumentPart slide = new POCO.DocumentPart();
                    slide.partnumber = i;
                    slide.body = slideText;
                    slide.header = string.Empty;
                    slide.footer = string.Empty;

                    // Add to our parts collection
                    text.parts.Add(slide);
                }

            }

            return text;
        }

        // Count the slides in the presentation.
        public static int CountSlides(PresentationDocument presentationDocument)
        {
            // Check for a null document object.
            if (presentationDocument == null)
            {
                throw new ArgumentNullException("presentationDocument");
            }

            int slidesCount = 0;

            // Get the presentation part of document.
            PresentationPart presentationPart = presentationDocument.PresentationPart;
            // Get the slide count from the SlideParts.
            if (presentationPart != null)
            {
                slidesCount = presentationPart.SlideParts.Count();
            }
            // Return the slide count to the previous method.
            return slidesCount;
        }

        public static string GetSlideIdAndText(PresentationDocument presentationDocument, int index)
        {
            // Get the relationship ID of the first slide.
            PresentationPart part = presentationDocument.PresentationPart;
            OpenXmlElementList slideIds = part.Presentation.SlideIdList.ChildElements;

            string relId = (slideIds[index] as SlideId).RelationshipId;

            // Get the slide part from the relationship ID.
            SlidePart slide = (SlidePart)part.GetPartById(relId);

            // Build a StringBuilder object.
            StringBuilder paragraphText = new StringBuilder();

            // Get the inner text of the slide:
            IEnumerable<A.Text> texts = slide.Slide.Descendants<A.Text>();
            foreach (A.Text text in texts)
            {
                paragraphText.Append(text.Text + Environment.NewLine);
            }

            return paragraphText.ToString();

        }

    }


}
