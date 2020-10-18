using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO.Compression;

using Microsoft.Extensions.Logging;

using DocumentFormat.OpenXml.Wordprocessing;
using A = DocumentFormat.OpenXml.Drawing;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml;
using System.Text;

using Microsoft.AspNetCore.Http;
using Castlepoint.POCO;
using System.IO;

namespace Castlepoint.Text
{
    

    public static class Word
    {
        public static POCO.DocumentText ExtractText(IFormFile upload, ILogger logger)
        {
            POCO.DocumentText text = new POCO.DocumentText();

            try
            {

                // Read the bytes from the stream
                System.IO.MemoryStream memstream = new System.IO.MemoryStream();
                upload.OpenReadStream().CopyTo(memstream);

                text = ProcessFile(memstream);



                return text;
            }
            catch(Exception exWordExtractText)
            {
                logger.LogError("Word Extract Text: " + exWordExtractText.Message);
                throw;
            }

        }

        private static DocumentText ProcessFile(MemoryStream memstream)
        {

            int runCounter = 0;

            POCO.DocumentText text = new POCO.DocumentText();

            try

            {
                // Open the document as read-only.
                using (WordprocessingDocument wordDocument = WordprocessingDocument.Open(memstream, false))
                {
                    // Check for a null document object.
                    if (wordDocument == null)
                    {
                        throw new ArgumentNullException("wordDocument");
                    }

                    int paraCounter = 0;
                    foreach (var paragraph in wordDocument.MainDocumentPart.RootElement.Descendants<Paragraph>())
                    {
                        paraCounter++;

                        string paraText = string.Empty;
                        foreach (var run in paragraph.Elements<Run>())
                        {
                            runCounter++;

                            foreach (var texttype in run.Elements<TextType>())
                            {
                                paraText += texttype.Text;
                            }
                            //string textContent = run.Elements<DocumentFormat.OpenXml.Wordprocessing.TextType>().Aggregate("", (s, t) => s + t.Text);

                        }

                        // Check if any text was found
                        if (paraText != string.Empty)
                        {

                            POCO.DocumentPart part = new POCO.DocumentPart();
                            part.body = paraText;
                            part.partnumber = paraCounter;
                            text.parts.Add(part);
                        }

                    }

                }
            }
            catch(OpenXmlPackageException packageEx)
            {
                if(packageEx.ToString().Contains("Invalid Hyperlink"))
            {
                    MemoryStream fixedMemStream = new MemoryStream();
                    fixedMemStream = Castlepoint.Text.FileHandlers.Utils.FixInvalidUri(memstream);
                    text = ProcessFile(fixedMemStream);
                }
            }

            return text;
        }

        public static POCO.DocumentText GetCommentsFromDocument(WordprocessingDocument wordDocument)
        {
            POCO.DocumentText text = new POCO.DocumentText();

            WordprocessingCommentsPart commentsPart =
                wordDocument.MainDocumentPart.WordprocessingCommentsPart;

            int counterComments = 0;
            if (commentsPart != null && commentsPart.Comments != null)
            {
                foreach (Comment comment in commentsPart.Comments.Elements<Comment>())
                {
                    counterComments++;
                    POCO.DocumentPart part = new POCO.DocumentPart();
                    part.body = comment.InnerText;
                    part.partnumber = counterComments;
                    text.parts.Add(part);
                }
            }

            return text;
        }

    }


}
