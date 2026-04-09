using System.Text.Json.Serialization;

namespace Vanguarr.Jellyfin.Models;

public sealed class VanguarrSuggestionResponse
{
    [JsonPropertyName("username")]
    public string Username { get; set; } = string.Empty;

    [JsonPropertyName("jellyfin_user_id")]
    public string JellyfinUserId { get; set; } = string.Empty;

    [JsonPropertyName("count")]
    public int Count { get; set; }

    [JsonPropertyName("items")]
    public List<VanguarrSuggestionItem> Items { get; set; } = [];
}

public sealed class VanguarrSuggestionItem
{
    [JsonPropertyName("rank")]
    public int Rank { get; set; }

    [JsonPropertyName("media_type")]
    public string MediaType { get; set; } = string.Empty;

    [JsonPropertyName("title")]
    public string Title { get; set; } = string.Empty;

    [JsonPropertyName("overview")]
    public string Overview { get; set; } = string.Empty;

    [JsonPropertyName("production_year")]
    public int? ProductionYear { get; set; }

    [JsonPropertyName("score")]
    public double Score { get; set; }

    [JsonPropertyName("reasoning")]
    public string Reasoning { get; set; } = string.Empty;

    [JsonPropertyName("state")]
    public string State { get; set; } = string.Empty;

    [JsonPropertyName("external_ids")]
    public VanguarrExternalIds ExternalIds { get; set; } = new();
}

public sealed class VanguarrExternalIds
{
    [JsonPropertyName("tmdb")]
    public string? Tmdb { get; set; }

    [JsonPropertyName("tvdb")]
    public string? Tvdb { get; set; }

    [JsonPropertyName("imdb")]
    public string? Imdb { get; set; }
}
