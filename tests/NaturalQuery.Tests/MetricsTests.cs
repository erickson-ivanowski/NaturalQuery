using System.Diagnostics.Metrics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using NaturalQuery.Diagnostics;
using NaturalQuery.Models;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-027: native metrics — query count, latency, token usage, cache hit rate,
/// error rate — observable via a standard .NET Meter/MeterListener pipeline.
/// Runs in its own xUnit collection (disabled parallelization) because the
/// NaturalQuery Meter is process-global — a MeterListener in this class would
/// otherwise also observe measurements emitted by engines in other test classes
/// running concurrently.
/// </summary>
[Collection("MetricsTests")]
public class MetricsTests : IDisposable
{
    private const string SuccessLlmJson = "{\"sql\":\"SELECT COUNT(*) AS value FROM users\",\"chartType\":\"metric\",\"title\":\"t\",\"description\":\"d\"}";

    private readonly Mock<ILlmProvider> _llmMock = new();
    private readonly Mock<IQueryExecutor> _executorMock = new();
    private readonly List<(string Name, object? Value, KeyValuePair<string, object?>[] Tags)> _measurements = new();
    private readonly MeterListener _listener;

    public MetricsTests()
    {
        _listener = new MeterListener();
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == NaturalQueryMetrics.MeterName)
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            _measurements.Add((instrument.Name, value, tags.ToArray().ToArray())));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            _measurements.Add((instrument.Name, value, tags.ToArray().ToArray())));
        _listener.Start();
    }

    public void Dispose() => _listener.Dispose();

    /// <summary>
    /// Unique per-test tenant tag: the NaturalQuery Meter is process-global, so
    /// even with collection-level isolation a test must filter to its own
    /// measurements rather than assume the list contains only its own activity.
    /// </summary>
    private readonly string _tenant = $"metrics-test-{Guid.NewGuid():N}";

    private NaturalQueryEngine CreateEngine()
    {
        var options = new NaturalQueryOptions();
        _llmMock
            .Setup(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LlmResponse(SuccessLlmJson, 42));
        _executorMock
            .Setup(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<DataPoint> { new("total", 7) });

        return new NaturalQueryEngine(
            _llmMock.Object,
            _executorMock.Object,
            Options.Create(options),
            NullLogger<NaturalQueryEngine>.Instance);
    }

    private IEnumerable<(string Name, object? Value, KeyValuePair<string, object?>[] Tags)> OwnMeasurements(string name) =>
        _measurements.Where(m => m.Name == name && m.Tags.Any(t => t.Key == "tenant" && (string?)t.Value == _tenant));

    [Fact]
    public async Task Successful_Query_Should_Record_Query_Count()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?", _tenant);

        OwnMeasurements("naturalquery.queries").Should().NotBeEmpty();
    }

    [Fact]
    public async Task Successful_Query_Should_Record_Duration_Histogram()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?", _tenant);

        OwnMeasurements("naturalquery.duration").Should().NotBeEmpty();
    }

    [Fact]
    public async Task Successful_Query_Should_Record_Token_Usage()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?", _tenant);

        var tokenMeasurement = OwnMeasurements("naturalquery.tokens").Single();
        Convert.ToInt64(tokenMeasurement.Value).Should().Be(42);
    }

    [Fact]
    public async Task Query_Outcome_Tag_Should_Reflect_Success()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?", _tenant);

        var queryMeasurement = OwnMeasurements("naturalquery.queries").Single();
        queryMeasurement.Tags.Should().Contain(t => t.Key == "outcome" && (string?)t.Value == "success");
    }

    [Fact]
    public async Task Failed_Query_Should_Record_Error_Outcome()
    {
        var engine = CreateEngine();
        _executorMock
            .Setup(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("boom"));

        await engine.Invoking(e => e.AskAsync("q", _tenant)).Should().ThrowAsync<Exception>();

        var queryMeasurement = OwnMeasurements("naturalquery.queries").Single();
        queryMeasurement.Tags.Should().Contain(t => t.Key == "outcome" && (string?)t.Value != "success");
    }

    [Fact]
    public async Task Cache_Hit_Should_Be_Recorded()
    {
        var cacheMock = new Mock<Caching.IQueryCache>();
        cacheMock
            .Setup(c => c.GetAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1" });
        var options = Options.Create(new NaturalQueryOptions());
        var engine = new NaturalQueryEngine(
            _llmMock.Object, _executorMock.Object, options,
            NullLogger<NaturalQueryEngine>.Instance, cache: cacheMock.Object);

        await engine.AskAsync("how many users?", _tenant);

        OwnMeasurements("naturalquery.cache").Should().Contain(m =>
            m.Tags.Any(t => t.Key == "result" && (string?)t.Value == "hit"));
    }
}

[CollectionDefinition("MetricsTests", DisableParallelization = true)]
public class MetricsTestsCollection { }
