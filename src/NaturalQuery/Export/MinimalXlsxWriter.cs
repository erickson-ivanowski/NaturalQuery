using System.Globalization;
using System.IO.Compression;
using System.Text;

namespace NaturalQuery.Export;

/// <summary>
/// Hand-rolled minimal SpreadsheetML (.xlsx) writer built directly on
/// System.IO.Compression.ZipArchive — no third-party Excel dependency. Produces
/// a single worksheet with a header row, inline strings, and numeric cell
/// detection. Sufficient for exporting a flat query result table.
/// </summary>
public static class MinimalXlsxWriter
{
    /// <summary>
    /// Writes a table (header + rows) as a minimal .xlsx workbook and returns the
    /// bytes.
    /// </summary>
    public static byte[] Write(IReadOnlyList<string> headers, IReadOnlyList<IReadOnlyList<string>> rows)
    {
        using var ms = new MemoryStream();
        using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            WriteEntry(archive, "[Content_Types].xml", ContentTypesXml());
            WriteEntry(archive, "_rels/.rels", RelsXml());
            WriteEntry(archive, "xl/workbook.xml", WorkbookXml());
            WriteEntry(archive, "xl/_rels/workbook.xml.rels", WorkbookRelsXml());
            WriteEntry(archive, "xl/worksheets/sheet1.xml", SheetXml(headers, rows));
        }
        return ms.ToArray();
    }

    private static void WriteEntry(ZipArchive archive, string name, string content)
    {
        var entry = archive.CreateEntry(name, CompressionLevel.Fastest);
        using var writer = new StreamWriter(entry.Open(), new UTF8Encoding(false));
        writer.Write(content);
    }

    private static string ContentTypesXml() =>
        """
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
          <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml" />
          <Default Extension="xml" ContentType="application/xml" />
          <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml" />
          <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml" />
        </Types>
        """;

    private static string RelsXml() =>
        """
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
          <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml" />
        </Relationships>
        """;

    private static string WorkbookXml() =>
        """
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
          <sheets>
            <sheet name="Sheet1" sheetId="1" r:id="rId1" />
          </sheets>
        </workbook>
        """;

    private static string WorkbookRelsXml() =>
        """
        <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
        <Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
          <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml" />
        </Relationships>
        """;

    private static string SheetXml(IReadOnlyList<string> headers, IReadOnlyList<IReadOnlyList<string>> rows)
    {
        var sb = new StringBuilder();
        sb.Append("""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>""");
        sb.Append("""<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>""");

        AppendRow(sb, 1, headers);

        var rowIndex = 2;
        foreach (var row in rows)
        {
            AppendRow(sb, rowIndex, row);
            rowIndex++;
        }

        sb.Append("</sheetData></worksheet>");
        return sb.ToString();
    }

    private static void AppendRow(StringBuilder sb, int rowIndex, IReadOnlyList<string> values)
    {
        sb.Append($"""<row r="{rowIndex}">""");
        for (var col = 0; col < values.Count; col++)
        {
            var reference = $"{ColumnLetter(col)}{rowIndex}";
            var value = values[col];

            if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var numeric))
            {
                sb.Append($"""<c r="{reference}"><v>{numeric.ToString(CultureInfo.InvariantCulture)}</v></c>""");
            }
            else
            {
                sb.Append($"""<c r="{reference}" t="inlineStr"><is><t xml:space="preserve">{Escape(value)}</t></is></c>""");
            }
        }
        sb.Append("</row>");
    }

    private static string ColumnLetter(int index)
    {
        var letters = "";
        index++;
        while (index > 0)
        {
            var remainder = (index - 1) % 26;
            letters = (char)('A' + remainder) + letters;
            index = (index - 1) / 26;
        }
        return letters;
    }

    private static string Escape(string value) => System.Security.SecurityElement.Escape(value) ?? value;
}
