using FluentAssertions;
using NaturalQuery.Extensions;
using NaturalQuery.Masking;
using NaturalQuery.Models;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-019: values of columns marked Sensitive are fully redacted (***) in every
/// output form — table data, chart data, and exports.
/// </summary>
public class SensitiveDataMaskerTests
{
    private static List<TableSchema> TablesWithSensitiveEmail() => new()
    {
        new TableSchema("users", new[]
        {
            new ColumnDef("id", "string"),
            new ColumnDef("email", "string", "customer email", sensitive: true),
            new ColumnDef("name", "string"),
        })
    };

    [Fact]
    public void Sensitive_Column_Values_Should_Be_Masked_In_TableData()
    {
        var result = new QueryResult
        {
            Sql = "SELECT id, email, name FROM users",
            TableData = new List<Dictionary<string, string>>
            {
                new() { ["id"] = "1", ["email"] = "a@x.com", ["name"] = "Alice" },
                new() { ["id"] = "2", ["email"] = "b@x.com", ["name"] = "Bob" },
            }
        };

        SensitiveDataMasker.Mask(result, TablesWithSensitiveEmail());

        result.TableData![0]["email"].Should().Be("***");
        result.TableData[1]["email"].Should().Be("***");
        result.TableData[0]["id"].Should().Be("1");
        result.TableData[0]["name"].Should().Be("Alice");
    }

    [Fact]
    public void Column_Matching_Should_Be_Case_Insensitive()
    {
        var result = new QueryResult
        {
            Sql = "SELECT EMAIL FROM users",
            TableData = new List<Dictionary<string, string>>
            {
                new() { ["EMAIL"] = "a@x.com" },
            }
        };

        SensitiveDataMasker.Mask(result, TablesWithSensitiveEmail());

        result.TableData![0]["EMAIL"].Should().Be("***");
    }

    [Fact]
    public void Chart_Grouping_On_Sensitive_Column_Should_Mask_Labels()
    {
        var result = new QueryResult
        {
            Sql = "SELECT email AS label, COUNT(*) AS value FROM users GROUP BY email",
            ChartType = "bar",
            ChartData = new List<DataPoint>
            {
                new("a@x.com", 3),
                new("b@x.com", 5),
            }
        };

        SensitiveDataMasker.Mask(result, TablesWithSensitiveEmail());

        result.ChartData![0].Label.Should().Be("***");
        result.ChartData[1].Label.Should().Be("***");
        result.ChartData[0].Value.Should().Be(3);
        result.ChartData[1].Value.Should().Be(5);
    }

    [Fact]
    public void Chart_On_Non_Sensitive_Column_Should_Not_Mask()
    {
        var result = new QueryResult
        {
            Sql = "SELECT name AS label, COUNT(*) AS value FROM users GROUP BY name",
            ChartType = "bar",
            ChartData = new List<DataPoint> { new("Alice", 3) }
        };

        SensitiveDataMasker.Mask(result, TablesWithSensitiveEmail());

        result.ChartData![0].Label.Should().Be("Alice");
    }

    [Fact]
    public void No_Sensitive_Columns_Should_Leave_Result_Untouched()
    {
        var tables = new List<TableSchema>
        {
            new("users", new[] { new ColumnDef("id", "string"), new ColumnDef("email", "string") })
        };
        var result = new QueryResult
        {
            Sql = "SELECT email FROM users",
            TableData = new List<Dictionary<string, string>> { new() { ["email"] = "a@x.com" } }
        };

        SensitiveDataMasker.Mask(result, tables);

        result.TableData![0]["email"].Should().Be("a@x.com");
    }

    [Fact]
    public void Exports_Should_Inherit_Masking()
    {
        var result = new QueryResult
        {
            Sql = "SELECT id, email FROM users",
            TableData = new List<Dictionary<string, string>>
            {
                new() { ["id"] = "1", ["email"] = "a@x.com" },
            }
        };

        SensitiveDataMasker.Mask(result, TablesWithSensitiveEmail());

        result.ToCsv().Should().Contain("***").And.NotContain("a@x.com");
        result.ToJson().Should().Contain("***").And.NotContain("a@x.com");
    }
}
