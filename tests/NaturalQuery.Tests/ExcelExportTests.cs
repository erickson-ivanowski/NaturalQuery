using System.IO.Compression;
using System.Xml.Linq;
using FluentAssertions;
using NaturalQuery.Extensions;
using NaturalQuery.Masking;
using NaturalQuery.Models;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-029: exporting query results to spreadsheet (Excel) format, alongside the
/// existing CSV/JSON exports. Round-trips the produced .xlsx as a zip archive.
/// </summary>
public class ExcelExportTests
{
    private static QueryResult SampleTableResult() => new()
    {
        Sql = "SELECT name, age FROM users",
        TableData = new List<Dictionary<string, string>>
        {
            new() { ["name"] = "Alice", ["age"] = "30" },
            new() { ["name"] = "Bob", ["age"] = "25" },
        }
    };

    [Fact]
    public void ToExcel_Should_Produce_A_Valid_Zip_Archive()
    {
        var bytes = SampleTableResult().ToExcel();

        using var stream = new MemoryStream(bytes);
        var act = () => new ZipArchive(stream, ZipArchiveMode.Read);

        act.Should().NotThrow();
    }

    [Fact]
    public void ToExcel_Should_Contain_A_Worksheet_Entry()
    {
        var bytes = SampleTableResult().ToExcel();

        using var stream = new MemoryStream(bytes);
        using var archive = new ZipArchive(stream, ZipArchiveMode.Read);

        archive.Entries.Should().Contain(e => e.FullName.Contains("sheet1.xml"));
        archive.Entries.Should().Contain(e => e.FullName == "[Content_Types].xml");
    }

    [Fact]
    public void ToExcel_Should_Include_Header_Row_And_Values_As_Inline_Strings()
    {
        var bytes = SampleTableResult().ToExcel();

        var sheetXml = ReadSheetXml(bytes);

        sheetXml.Should().Contain("name").And.Contain("age");
        sheetXml.Should().Contain("Alice").And.Contain("Bob");
    }

    [Fact]
    public void ToExcel_Should_Write_Numeric_Values_As_Number_Cells()
    {
        var bytes = SampleTableResult().ToExcel();

        var doc = XDocument.Parse(ReadSheetXml(bytes));
        var ns = doc.Root!.GetDefaultNamespace();

        // A numeric cell has no "t" attribute (or t="n"); a string cell has t="inlineStr" or t="str".
        var ageCells = doc.Descendants(ns + "c")
            .Where(c => c.Element(ns + "v")?.Value is "30" or "25");

        ageCells.Should().NotBeEmpty();
        ageCells.Should().OnlyContain(c => (string?)c.Attribute("t") != "inlineStr");
    }

    [Fact]
    public void ToExcelStream_Should_Produce_Equivalent_Bytes_To_ToExcel()
    {
        var result = SampleTableResult();

        var bytes = result.ToExcel();
        using var stream = result.ToExcelStream();
        using var ms = new MemoryStream();
        stream.CopyTo(ms);

        ms.ToArray().Should().Equal(bytes);
    }

    [Fact]
    public void ToExcel_Should_Preserve_Masked_Values_As_Literal_Asterisks()
    {
        var result = SampleTableResult();
        var tables = new List<TableSchema>
        {
            new("users", new[] { new ColumnDef("name", "string", "n", sensitive: true), new ColumnDef("age", "int") })
        };
        SensitiveDataMasker.Mask(result, tables);

        var sheetXml = ReadSheetXml(result.ToExcel());

        sheetXml.Should().Contain("***");
        sheetXml.Should().NotContain("Alice").And.NotContain("Bob");
    }

    [Fact]
    public void ToExcel_On_Chart_Data_Should_Export_Label_Value_Columns()
    {
        var result = new QueryResult
        {
            Sql = "SELECT status AS label, COUNT(*) AS value FROM users GROUP BY status",
            ChartType = "bar",
            ChartData = new List<DataPoint> { new("active", 10), new("inactive", 3) }
        };

        var sheetXml = ReadSheetXml(result.ToExcel());

        sheetXml.Should().Contain("active").And.Contain("inactive");
    }

    private static string ReadSheetXml(byte[] xlsxBytes)
    {
        using var stream = new MemoryStream(xlsxBytes);
        using var archive = new ZipArchive(stream, ZipArchiveMode.Read);
        var entry = archive.Entries.Single(e => e.FullName.Contains("sheet1.xml"));
        using var reader = new StreamReader(entry.Open());
        return reader.ReadToEnd();
    }
}
