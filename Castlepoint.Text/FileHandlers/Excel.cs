using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using ExcelDataReader;

using DocumentFormat.OpenXml.Spreadsheet;
using A = DocumentFormat.OpenXml.Drawing;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml;
using System.Text;

using Microsoft.AspNetCore.Http;
using Castlepoint.POCO;
using System.IO;

namespace Castlepoint.Text
{    

    public static class Excel
    {

        public static POCO.DocumentText ExtractText(IFormFile upload, ILogger logger)
        {

            POCO.DocumentText text = new POCO.DocumentText();

            try
            {

                // Read the bytes from the stream
                System.IO.MemoryStream memstream = new System.IO.MemoryStream();
                upload.OpenReadStream().CopyTo(memstream);

                text = ProcessFile2(memstream, upload.FileName, logger);

                return text;
            }
            catch (Exception exWordExtractText)
            {
                logger.LogError("Word Extract Text: " + exWordExtractText.Message);
                throw;
            }


            return text;
        }

        private static DocumentText ProcessFile2(MemoryStream memstream, string fileName, ILogger logger)
        {

            int runCounter = 0;

            POCO.DocumentText text = new POCO.DocumentText();

            // Auto-detect format, supports:
            //  - Binary Excel files (2.0-2003 format; *.xls)
            //  - OpenXml Excel files (2007 format; *.xlsx)
            using (var reader = ExcelReaderFactory.CreateReader(memstream))
            {
                var result = reader.AsDataSet();
                logger.LogInformation("ProcessFile: tables=" + result.Tables.Count.ToString() + " filename=" + fileName);

                int sheetCounter = 0;
                foreach(System.Data.DataTable table in result.Tables)
                {
                    sheetCounter++;
                    logger.LogInformation("ProcessFile: processing sheet#" + sheetCounter.ToString() + " filename=" + fileName);

                    StringBuilder sbSheet = new StringBuilder();

                    foreach (System.Data.DataRow row in table.Rows)
                    {
                        // Append each row of data as tab-separated
                        sbSheet.Append(string.Join("\t", row.ItemArray));
                        sbSheet.AppendLine();
                    }

                    // Add this sheet as a new part
                    POCO.DocumentPart part = new POCO.DocumentPart();
                    part.body = sbSheet.ToString();
                    part.partnumber = sheetCounter;
                    text.parts.Add(part);

                }
            }

            return text;
        }

        // 20200502 GM REPLACED very slow performance due to continuous SharedString table lookups
        //    private static DocumentText ProcessFile(MemoryStream memstream, string fileName, ILogger logger)
        //{

        //    int runCounter = 0;

        //    POCO.DocumentText text = new POCO.DocumentText();

        //    try
        //    {
        //        logger.LogInformation("ProcessFile: START filename=" + fileName);
        //        // Open the presentation as read-only.
        //        using (SpreadsheetDocument excelDocument = SpreadsheetDocument.Open(memstream, false))
        //        {
        //            // Check for a null document object.
        //            if (excelDocument == null)
        //            {
        //                throw new ArgumentNullException("excelDocument");
        //            }

        //            // Access the workbook section
        //            WorkbookPart workbookPart = excelDocument.WorkbookPart;

        //            // Get the workbook shared strings
        //            var stringTable = workbookPart.GetPartsOfType<SharedStringTablePart>().FirstOrDefault();
        //            logger.LogInformation("ProcessFile: loaded shared string table filename=" + fileName);

        //            // Process each worksheet
        //            int sheetCounter = 0;
        //            foreach (WorksheetPart workSheetPart in workbookPart.WorksheetParts)
        //            {
        //                sheetCounter++;
        //                logger.LogInformation("ProcessFile: worksheet=" + sheetCounter.ToString() + " filename=" + fileName);

        //                // Read all Cell Values
        //                OpenXmlReader reader = OpenXmlReader.Create(workSheetPart);
        //                string sheettext = string.Empty;
        //                int counter = 0;
        //                int counterData = 0;
        //                while (reader.Read())
        //                {
        //                    counter++;
        //                    if (counter % 1000 == 0)
        //                    {
        //                        logger.LogInformation("ProcessFile: read data counter=" + counter.ToString());
        //                    }
        //                    if (reader.ElementType == typeof(Cell))
        //                    {
        //                        // Get the cell value and number
        //                        Cell c = (Cell)reader.LoadCurrentElement();
        //                        string cellValue = ExcelHelper.GetCellValue(c, stringTable, workbookPart);
        //                        if (cellValue.Trim().Length > 0)
        //                        {
        //                            sheettext += cellValue.Trim() + " ";
        //                            counterData++;
        //                        }
        //                    }
        //                }

        //                logger.LogInformation("ProcessFile: part #" + sheetCounter.ToString() + " data cells=" + counterData.ToString());

        //                // Check if any text was found
        //                if (sheettext != string.Empty)
        //                {
        //                    POCO.DocumentPart part = new POCO.DocumentPart();
        //                    part.body = sheettext;
        //                    part.partnumber = sheetCounter;
        //                    text.parts.Add(part);
        //                }

        //            }



        //        }
        //    }
        //    catch (OpenXmlPackageException packageEx)
        //    {
        //        logger.LogWarning("ProcessFile: package exception filename=" + fileName);
        //        if (packageEx.ToString().Contains("Invalid Hyperlink"))
        //        {
        //            logger.LogWarning("ProcessFile: fixing mem stream filename=" + fileName);
        //            MemoryStream fixedMemStream = new MemoryStream();
        //            fixedMemStream = Castlepoint.Text.FileHandlers.Utils.FixInvalidUri(memstream);
        //            text = ProcessFile(fixedMemStream, fileName, logger);
        //        }
        //    }
        //    catch(Exception ex)
        //    {
        //        logger.LogError("ProcessFile: package exception filename=" + fileName + " [" + ex.Message + "]");
        //    }

        //    return text;


        //}
    }


}
