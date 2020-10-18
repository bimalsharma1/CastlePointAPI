using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Spreadsheet;
using DocumentFormat.OpenXml.Packaging;

using System.Text.RegularExpressions;

namespace Castlepoint.Utilities
{
    public class ExcelHelper
    {
        static uint[] builtInDateTimeNumberFormatIDs = new uint[] { 14, 15, 16, 17, 18, 19, 20, 21, 22, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 45, 46, 47, 50, 51, 52, 53, 54, 55, 56, 57, 58 };
        static Dictionary<uint, NumberingFormat> builtInDateTimeNumberFormats = builtInDateTimeNumberFormatIDs.ToDictionary(id => id, id => new NumberingFormat { NumberFormatId = id });
        static Regex dateTimeFormatRegex = new Regex(@"((?=([^[]*\[[^[\]]*\])*([^[]*[ymdhs]+[^\]]*))|.*\[(h|mm|ss)\].*)", RegexOptions.Compiled);

        public static Dictionary<uint, NumberingFormat> GetDateTimeCellFormats(WorkbookPart workbookPart)
        {
            Dictionary<uint, NumberingFormat> dateNumberFormats = new Dictionary<uint, NumberingFormat>();    //new IEnumerable<NumberingFormat>();
            var numberFormats = workbookPart.WorkbookStylesPart.Stylesheet.NumberingFormats;
            if (numberFormats != null)
            {
                dateNumberFormats = numberFormats
                    .Descendants<NumberingFormat>()
                    .Where(nf => dateTimeFormatRegex.Match(nf.FormatCode.Value).Success)
                    .ToDictionary(nf => nf.NumberFormatId.Value);
            }

            var cellFormats = workbookPart.WorkbookStylesPart.Stylesheet.CellFormats
                .Descendants<CellFormat>();

            var dateCellFormats = new Dictionary<uint, NumberingFormat>();
            uint styleIndex = 0;
            foreach (var cellFormat in cellFormats)
            {
                if (cellFormat.ApplyNumberFormat != null && cellFormat.ApplyNumberFormat.Value)
                {
                    // Check if NumberFormatId is valid
                    if (cellFormat.NumberFormatId != null)
                    {
                        if (dateNumberFormats.ContainsKey(cellFormat.NumberFormatId.Value))
                        {
                            dateCellFormats.Add(styleIndex, dateNumberFormats[cellFormat.NumberFormatId.Value]);
                        }
                        else if (builtInDateTimeNumberFormats.ContainsKey(cellFormat.NumberFormatId.Value))
                        {
                            dateCellFormats.Add(styleIndex, builtInDateTimeNumberFormats[cellFormat.NumberFormatId.Value]);
                        }
                    }
                }

                styleIndex++;
            }

            return dateCellFormats;
        }

        // Usage Example
        public static bool IsDateTimeCell(WorkbookPart workbookPart, Cell cell)
        {
            if (cell.StyleIndex == null)
                return false;

            var dateTimeCellFormats = ExcelHelper.GetDateTimeCellFormats(workbookPart);

            return dateTimeCellFormats.ContainsKey(cell.StyleIndex);
        }

        // Given text and a SharedStringTablePart, creates a SharedStringItem with the specified text 
        // and inserts it into the SharedStringTablePart. If the item already exists, returns its index.
        public static int InsertSharedStringItem(string text, SharedStringTablePart shareStringPart)
        {
            // If the part does not contain a SharedStringTable, create one.
            if (shareStringPart.SharedStringTable == null)
            {
                shareStringPart.SharedStringTable = new SharedStringTable();
            }

            int i = 0;

            // Iterate through all the items in the SharedStringTable. If the text already exists, return its index.
            foreach (SharedStringItem item in shareStringPart.SharedStringTable.Elements<SharedStringItem>())
            {
                if (item.InnerText == text)
                {
                    return i;
                }

                i++;
            }

            // The text does not exist in the part. Create the SharedStringItem and return its index.
            shareStringPart.SharedStringTable.AppendChild(new SharedStringItem(new DocumentFormat.OpenXml.Spreadsheet.Text(text)));
            shareStringPart.SharedStringTable.Save();

            return i;
        }

        public static Cell CreateSharedStringCell(string text, SharedStringTablePart shareStringPart)
        {
            // Create the new cell
            Cell c = new Cell();

            // Insert the text into the SharedStringTablePart.
            int index = ExcelHelper.InsertSharedStringItem(text, shareStringPart);

            // Add the indexed shared string to the cell
            c.DataType = DocumentFormat.OpenXml.Spreadsheet.CellValues.SharedString;
            c.CellValue = new CellValue(index.ToString());

            return c;
        }

        // Retrieve the value of a cell, given a file name, sheet name, 
        // and address name.
        public static string GetCellValue(Cell theCell, SharedStringTablePart stringTable, WorkbookPart bookPart)
        {
            string value = null;

            // If the cell does not exist, return an empty string.
            if (theCell != null)
            {
                value = theCell.InnerText;
                if (value.Length == 0)
                {
                    // No data to process
                    return value;
                }

                if (ExcelHelper.IsDateTimeCell(bookPart, theCell))
                {
                    // Set the value as a date time
                    DateTime newDate = DateTime.FromOADate(double.Parse(value));
                    value = newDate.ToString();
                    return value;
                }

                // If the cell represents an integer number, you are done. 
                // For dates, this code returns the serialized value that 
                // represents the date. The code handles strings and 
                // Booleans individually. For shared strings, the code 
                // looks up the corresponding value in the shared string 
                // table. For Booleans, the code converts the value into 
                // the words TRUE or FALSE.
                if (theCell.DataType != null)
                {
                    switch (theCell.DataType.Value)
                    {
                        case CellValues.SharedString:

                            // If the shared string table is missing, something 
                            // is wrong. Return the index that is in
                            // the cell. Otherwise, look up the correct text in 
                            // the table.
                            if (stringTable != null)
                            {
                                value =
                                    stringTable.SharedStringTable
                                    .ElementAt(int.Parse(value)).InnerText;
                            }
                            else
                            {
                                throw new ApplicationException("The shared string table has not been supplied for the GetCellValue process");
                            }
                            break;

                        case CellValues.Boolean:
                            switch (value)
                            {
                                case "0":
                                    value = "FALSE";
                                    break;
                                default:
                                    value = "TRUE";
                                    break;
                            }
                            break;
                    }
                }
            }

            return value;
        }

        public static string GetColumnIdFromCellReference(Cell c)
        {
            string columnId = "";
            int number;
            foreach (char character in c.CellReference.Value.ToCharArray())
            {
                if (!int.TryParse(character.ToString(), out number))
                {
                    // Character is not a number - add to columnId
                    columnId += character.ToString();
                }
            }
            return columnId;

        }
    }
}
