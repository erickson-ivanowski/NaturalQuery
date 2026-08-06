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
/// </summary>
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

    [Fact]
    public async Task Successful_Query_Should_Record_Query_Count()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?");

        _measurements.Should().Contain(m => m.Name == "naturalquery.queries");
    }

    [Fact]
    public async Task Successful_Query_Should_Record_Duration_Histogram()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?");

        _measurements.Should().Contain(m => m.Name == "naturalquery.duration");
    }

    [Fact]
    public async Task Successful_Query_Should_Record_Token_Usage()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?");

        var tokenMeasurement = _measurements.FirstOrDefault(m => m.Name == "naturalquery.tokens");
        tokenMeasurement.Name.Should().Be("naturalquery.tokens");
        Convert.ToInt64(tokenMeasurement.Value).Should().Be(42);
    }

    [Fact]
    public async Task Query_Outcome_Tag_Should_Reflect_Success()
    {
        var engine = CreateEngine();

        await engine.AskAsync("how many users?");

        var queryMeasurement = _measurements.First(m => m.Name == "naturalquery.queries");
        queryMeasurement.Tags.Should().Contain(t => t.Key == "outcome" && (string?)t.Value == "success");
    }

    [Fact]
    public async Task Failed_Query_Should_Record_Error_Outcome()
    {
        var engine = CreateEngine();
        _executorMock
            .Setup(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("boom"));

        await engine.Invoking(e => e.AskAsync("q")).Should().ThrowAsync<Exception>();

        var queryMeasurement = _measurements.First(m => m.Name == "naturalquery.queries");
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

        await engine.AskAsync("how many users?");

        _measurements.Should().Contain(m => m.Name == "naturalquery.cache" &&
            m.Tags.Any(t => t.Key == "result" && (string?)t.Value == "hit"));
    }
}
