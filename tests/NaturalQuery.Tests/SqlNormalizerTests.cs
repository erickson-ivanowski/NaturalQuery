using FluentAssertions;
using NaturalQuery.Validation;

namespace NaturalQuery.Tests;

public class SqlNormalizerTests
{
    // --- Line comments ---

    [Fact]
    public void Line_Comment_Should_Be_Stripped()
    {
        var result = SqlNormalizer.Normalize("SELECT 1 -- everything here goes\nFROM t");
        result.Should().Be("SELECT 1 FROM t");
    }

    [Fact]
    public void Line_Comment_At_End_Without_Newline_Should_Be_Stripped()
    {
        var result = SqlNormalizer.Normalize("SELECT 1 FROM t -- trailing");
        result.Should().Be("SELECT 1 FROM t");
    }

    [Fact]
    public void Line_Comment_Marker_Inside_Literal_Should_Not_Strip()
    {
        var result = SqlNormalizer.Normalize("SELECT * FROM t WHERE a = '--not a comment' AND b = 2");
        result.Should().Be("SELECT * FROM t WHERE a = '' AND b = 2");
    }

    // --- Block comments ---

    [Fact]
    public void Block_Comment_Should_Be_Stripped()
    {
        var result = SqlNormalizer.Normalize("SELECT/*hidden*/1 FROM t");
        result.Should().Be("SELECT 1 FROM t");
    }

    [Fact]
    public void Nested_Block_Comments_Should_Be_Stripped_Until_Stable()
    {
        var result = SqlNormalizer.Normalize("SELECT /*/* inner */ still comment */ 1 FROM t");
        result.Should().Be("SELECT 1 FROM t");
    }

    [Fact]
    public void Unterminated_Block_Comment_Should_Strip_To_End()
    {
        var result = SqlNormalizer.Normalize("SELECT 1 FROM t /* never closed");
        result.Should().Be("SELECT 1 FROM t");
    }

    [Fact]
    public void Block_Comment_Marker_Inside_Literal_Should_Not_Strip()
    {
        var result = SqlNormalizer.Normalize("SELECT * FROM t WHERE a = '/*x*/' AND b = 2");
        result.Should().Be("SELECT * FROM t WHERE a = '' AND b = 2");
    }

    // --- String literals ---

    [Fact]
    public void String_Literal_Should_Be_Emptied()
    {
        var result = SqlNormalizer.Normalize("SELECT * FROM t WHERE a = 'DELETE FROM users'");
        result.Should().Be("SELECT * FROM t WHERE a = ''");
    }

    [Fact]
    public void Escaped_Quote_Inside_Literal_Should_Be_Handled()
    {
        var result = SqlNormalizer.Normalize("SELECT * FROM t WHERE name = 'O''Brien' AND x = 1");
        result.Should().Be("SELECT * FROM t WHERE name = '' AND x = 1");
    }

    [Fact]
    public void Semicolon_Inside_Literal_Should_Be_Removed_With_Literal()
    {
        var result = SqlNormalizer.Normalize("SELECT * FROM t WHERE name = 'a;b'");
        result.Should().Be("SELECT * FROM t WHERE name = ''");
    }

    [Fact]
    public void Unterminated_Literal_Should_Strip_To_End()
    {
        var result = SqlNormalizer.Normalize("SELECT * FROM t WHERE a = 'oops");
        result.Should().Be("SELECT * FROM t WHERE a = ''");
    }

    // --- Whitespace ---

    [Fact]
    public void Tabs_And_Newlines_Should_Collapse_To_Single_Spaces()
    {
        var result = SqlNormalizer.Normalize("SELECT\t*\r\nFROM\n\n  t");
        result.Should().Be("SELECT * FROM t");
    }

    [Fact]
    public void Leading_And_Trailing_Whitespace_Should_Be_Trimmed()
    {
        var result = SqlNormalizer.Normalize("   SELECT 1   ");
        result.Should().Be("SELECT 1");
    }

    [Fact]
    public void Empty_Input_Should_Return_Empty()
    {
        SqlNormalizer.Normalize("").Should().Be(string.Empty);
    }

    [Fact]
    public void Newline_Split_Keyword_Should_Normalize_To_Detectable_Form()
    {
        var result = SqlNormalizer.Normalize("DELETE\nFROM users");
        result.Should().Be("DELETE FROM users");
    }

    [Fact]
    public void Comment_Should_Act_As_Token_Separator_Not_Concatenator()
    {
        // Replacing a comment with nothing would merge adjacent tokens ("SELECTname").
        var result = SqlNormalizer.Normalize("SELECT/*c*/name FROM t");
        result.Should().Be("SELECT name FROM t");
    }
}
