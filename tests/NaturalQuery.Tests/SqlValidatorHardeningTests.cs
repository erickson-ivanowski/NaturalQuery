using FluentAssertions;
using NaturalQuery.Validation;

namespace NaturalQuery.Tests;

/// <summary>
/// SC-001 corpus: obfuscated bypass attempts must be rejected, dialect-specific
/// dangerous operations must be rejected, and the legitimate read corpus must pass.
/// </summary>
public class SqlValidatorHardeningTests
{
    // --- Obfuscated bypasses (previously slipped through) ---

    [Theory]
    [InlineData("DELETE/**/FROM users")]
    [InlineData("DELETE\nFROM users")]
    [InlineData("DELETE\tFROM users")]
    [InlineData("DeLeTe FROM users WHERE id = 1")]
    [InlineData("/* harmless */ DELETE FROM users")]
    [InlineData("UPDATE\nusers SET x = 1")]
    [InlineData("WITH x AS (SELECT 1)\nINSERT INTO t SELECT * FROM x")]
    [InlineData("SELECT 1; DROP TABLE users --")]
    [InlineData("SELECT 1 /*;*/; DELETE FROM t")]
    [InlineData("SELECT/**/1;/**/DELETE/**/FROM/**/t")]
    [InlineData("TRUNCATE\n\nTABLE users")]
    public void Obfuscated_Dangerous_Query_Should_Be_Rejected(string sql)
    {
        SqlValidator.Validate(sql).Should().NotBeNull();
    }

    // --- Expanded dangerous-operation denylist ---

    [Theory]
    [InlineData("MERGE INTO users u USING src s ON u.id = s.id WHEN MATCHED THEN UPDATE SET u.x = 1")]
    [InlineData("SELECT 1; EXEC xp_cmdshell 'dir'")]
    [InlineData("SELECT 1; EXECUTE sp_executesql N'DROP TABLE t'")]
    [InlineData("SELECT 1; CALL dangerous_proc()")]
    [InlineData("ATTACH DATABASE '/tmp/evil.db' AS evil")]
    [InlineData("SELECT 1; DETACH DATABASE evil")]
    [InlineData("PRAGMA writable_schema = 1")]
    [InlineData("COPY users TO '/tmp/dump.csv'")]
    [InlineData("SELECT 1; VACUUM")]
    [InlineData("SELECT 1; REINDEX users")]
    [InlineData("LOAD DATA INFILE '/etc/passwd' INTO TABLE t")]
    [InlineData("SELECT * FROM users INTO OUTFILE '/tmp/x'")]
    [InlineData("SELECT * FROM users INTO DUMPFILE '/tmp/x'")]
    [InlineData("SELECT * INTO new_table FROM users")]
    [InlineData("SELECT * FROM OPENROWSET('SQLNCLI', 'conn', 'SELECT 1')")]
    [InlineData("SELECT * FROM OPENQUERY(server, 'SELECT 1')")]
    [InlineData("SELECT xp_cmdshell('dir')")]
    [InlineData("SELECT sp_configure('show advanced options', 1)")]
    [InlineData("GRANT ALL ON users TO PUBLIC")]
    [InlineData("REVOKE ALL ON users FROM PUBLIC")]
    public void Dangerous_Dialect_Operation_Should_Be_Rejected(string sql)
    {
        SqlValidator.Validate(sql).Should().NotBeNull();
    }

    // --- Multi-statement tricks ---

    [Theory]
    [InlineData("SELECT 1; SELECT 2")]
    [InlineData("SELECT 1;\nSELECT 2")]
    [InlineData("SELECT 1 /* c */; SELECT 2")]
    public void Multi_Statement_Should_Be_Rejected(string sql)
    {
        SqlValidator.Validate(sql).Should().NotBeNull();
    }

    // --- Legitimate corpus (zero false positives — FR-003) ---

    [Theory]
    [InlineData("SELECT * FROM events WHERE type IN ('INSERT', 'MODIFY')")]
    [InlineData("SELECT * FROM logs WHERE message = 'DROP TABLE attempted'")]
    [InlineData("SELECT * FROM audit WHERE action = 'DELETE FROM cart'")]
    [InlineData("SELECT updated_at, created_at, deleted_flag FROM t")]
    [InlineData("SELECT table_name FROM information_schema.tables")]
    [InlineData("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")]
    [InlineData("SELECT 'O''Brien' AS name FROM customers")]
    [InlineData("SELECT * FROM users WHERE note = 'a;b'")]
    [InlineData("SELECT * FROM t -- fetch everything\nWHERE id = 1")]
    [InlineData("/* daily report */ SELECT COUNT(*) AS value FROM orders")]
    [InlineData("WITH latest AS (SELECT * FROM users) SELECT * FROM latest")]
    [InlineData("SELECT * FROM users;")]
    [InlineData("SELECT exec_time, call_count FROM metrics")]
    [InlineData("SELECT merge_status FROM pipelines")]
    public void Legitimate_Read_Query_Should_Be_Accepted(string sql)
    {
        SqlValidator.Validate(sql).Should().BeNull();
    }

    // --- Word-boundary correctness (substring matching must not fire inside identifiers) ---

    [Theory]
    [InlineData("SELECT last_update FROM t")]
    [InlineData("SELECT pragma_version FROM t")]
    [InlineData("SELECT vacuum_count FROM stats")]
    [InlineData("SELECT copyright FROM books")]
    public void Identifier_Containing_Keyword_Should_Be_Accepted(string sql)
    {
        SqlValidator.Validate(sql).Should().BeNull();
    }

    // --- Existing behavior retained ---

    [Fact]
    public void Additional_Forbidden_Keywords_Still_Honored()
    {
        var result = SqlValidator.Validate(
            "SELECT * FROM users UNION SELECT * FROM admins",
            additionalForbidden: new[] { "UNION " });
        result.Should().Contain("Forbidden");
    }

    [Fact]
    public void Dangerous_Word_In_Literal_Plus_Real_Danger_Should_Still_Be_Rejected()
    {
        // Literal-stripping must not mask the genuine threat elsewhere in the query.
        var result = SqlValidator.Validate("SELECT 1; DELETE FROM t WHERE note = 'INSERT'");
        result.Should().NotBeNull();
    }

    [Fact]
    public void Cross_Dialect_Comment_Trick_Should_Be_Rejected()
    {
        // Under nesting dialects (PostgreSQL/T-SQL) this reads as "SELECT 1;" plus one
        // big comment; under MySQL/SQLite the comment ends at the first */ and a DELETE
        // would execute. Validation must reject it because ONE interpretation is dangerous.
        SqlValidator.Validate("SELECT 1; /*/*c*/ DELETE FROM t --*/").Should().NotBeNull();
    }

    [Fact]
    public void Tenant_Contract_Unchanged()
    {
        SqlValidator.Validate("SELECT * FROM users", tenantIdColumn: "tenant_id", tenantId: "abc")
            .Should().Contain("tenant");
        SqlValidator.Validate("SELECT * FROM users WHERE tenant_id = 'abc'", tenantIdColumn: "tenant_id", tenantId: "abc")
            .Should().BeNull();
    }
}
