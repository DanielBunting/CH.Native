using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace CH.Native.SystemTests.Fixtures;

/// <summary>
/// Minimal Toxiproxy admin-API client. We avoid the abandoned Toxiproxy.Net package and
/// hit the documented HTTP API directly.
/// </summary>
public sealed class ToxiproxyClient : IAsyncDisposable
{
    private readonly HttpClient _http;

    public ToxiproxyClient(string adminBaseUrl)
    {
        _http = new HttpClient { BaseAddress = new Uri(adminBaseUrl) };
    }

    public async Task EnsureProxyAsync(string name, string listen, string upstream)
    {
        // PUT-like upsert: delete-then-create keeps the test deterministic.
        await _http.DeleteAsync($"/proxies/{name}");
        var resp = await _http.PostAsJsonAsync("/proxies", new ProxyDto(name, listen, upstream, true));
        resp.EnsureSuccessStatusCode();
    }

    /// <summary>Add a toxic. <paramref name="type"/> is e.g. "latency", "bandwidth", "timeout", "reset_peer", "slow_close".</summary>
    public async Task<string> AddToxicAsync(string proxyName, string type, string stream,
        Dictionary<string, object> attributes, double toxicity = 1.0)
    {
        var name = $"{type}_{Guid.NewGuid():N}".Substring(0, 24);
        var resp = await _http.PostAsJsonAsync($"/proxies/{proxyName}/toxics",
            new ToxicDto(name, type, stream, toxicity, attributes));
        resp.EnsureSuccessStatusCode();
        return name;
    }

    public async Task RemoveAllToxicsAsync(string proxyName)
    {
        var toxics = await _http.GetFromJsonAsync<List<ToxicListEntry>>($"/proxies/{proxyName}/toxics");
        if (toxics is null) return;
        foreach (var t in toxics)
            await _http.DeleteAsync($"/proxies/{proxyName}/toxics/{t.Name}");
    }

    public ValueTask DisposeAsync()
    {
        _http.Dispose();
        return ValueTask.CompletedTask;
    }

    private sealed record ProxyDto(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("listen")] string Listen,
        [property: JsonPropertyName("upstream")] string Upstream,
        [property: JsonPropertyName("enabled")] bool Enabled);

    private sealed record ToxicDto(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("type")] string Type,
        [property: JsonPropertyName("stream")] string Stream,
        [property: JsonPropertyName("toxicity")] double Toxicity,
        [property: JsonPropertyName("attributes")] Dictionary<string, object> Attributes);

    private sealed record ToxicListEntry(
        [property: JsonPropertyName("name")] string Name);
}
