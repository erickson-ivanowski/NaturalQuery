namespace NaturalQuery.Models;

/// <summary>
/// Defines a database table schema for the LLM to generate queries against.
/// </summary>
public class TableSchema
{
    /// <summary>Table name as it appears in the database.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Optional description to help the LLM understand the table's purpose.</summary>
    public string? Description { get; set; }

    /// <summary>Column definitions.</summary>
    public List<ColumnDef> Columns { get; set; } = new();

    /// <summary>Partition columns (e.g., for Athena/Hive).</summary>
    public List<string> Partitions { get; set; } = new();

    public TableSchema() { }

    public TableSchema(string name, IEnumerable<ColumnDef> columns, string? description = null)
    {
        Name = name;
        Description = description;
        Columns = columns.ToList();
    }
}

/// <summary>
/// Defines a single column in a table schema.
/// </summary>
public class ColumnDef
{
    /// <summary>Column name as it appears in the database.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Column type (string, int, boolean, double, date, etc.).</summary>
    public string Type { get; set; } = "string";

    /// <summary>Optional description to help the LLM understand the column.</summary>
    public string? Description { get; set; }

    /// <summary>When true, values of this column are fully redacted (***) in every output form.</summary>
    public bool Sensitive { get; init; }

    public ColumnDef() { }

    public ColumnDef(string name, string type, string? description = null)
    {
        Name = name;
        Type = type;
        Description = description;
    }

    public ColumnDef(string name, string type, string? description, bool sensitive)
        : this(name, type, description)
    {
        Sensitive = sensitive;
    }
}
