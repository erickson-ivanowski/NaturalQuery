using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NaturalQuery.Extensions;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-023a: OpenRouter as a thin convenience over the OpenAI-compatible provider —
/// custom base address plus the recommended attribution headers, no new provider type.
/// </summary>
public class OpenRouterConfigurationTests
{
    [Fact]
    public void Client_Should_Target_OpenRouter_Api_Base()
    {
        using var client = OpenRouterDefaults.CreateHttpClient();

        client.BaseAddress.Should().Be(new Uri("https://openrouter.ai/api/"));
    }

    [Fact]
    public void Chat_Completions_Path_Should_Resolve_Under_OpenRouter_Base()
    {
        using var client = OpenRouterDefaults.CreateHttpClient();

        // OpenAiProvider posts to the relative path "v1/chat/completions"
        var resolved = new Uri(client.BaseAddress!, "v1/chat/completions");

        resolved.AbsoluteUri.Should().Be("https://openrouter.ai/api/v1/chat/completions");
    }

    [Fact]
    public void Attribution_Headers_Should_Be_Set_When_Provided()
    {
        using var client = OpenRouterDefaults.CreateHttpClient(
            referer: "https://myapp.example.com",
            title: "My App");

        client.DefaultRequestHeaders.GetValues("HTTP-Referer").Should().ContainSingle("https://myapp.example.com");
        client.DefaultRequestHeaders.GetValues("X-Title").Should().ContainSingle("My App");
    }

    [Fact]
    public void Attribution_Headers_Should_Be_Absent_When_Not_Provided()
    {
        using var client = OpenRouterDefaults.CreateHttpClient();

        client.DefaultRequestHeaders.Contains("HTTP-Referer").Should().BeFalse();
        client.DefaultRequestHeaders.Contains("X-Title").Should().BeFalse();
    }

    [Fact]
    public void UseOpenRouterProvider_Should_Register_OpenAiProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNaturalQuery(_ => { })
            .UseOpenRouterProvider("test-key", "anthropic/claude-sonnet-4.5");

        using var sp = services.BuildServiceProvider();
        var provider = sp.GetRequiredService<ILlmProvider>();

        provider.Should().BeOfType<OpenAiProvider>();
    }
}
