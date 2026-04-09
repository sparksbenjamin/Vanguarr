using System.Net.Http.Headers;
using System.Text.Json;
using Jellyfin.Database.Implementations.Entities;
using Jellyfin.Data.Enums;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Playlists;
using MediaBrowser.Model.Playlists;
using MediaBrowser.Model.Querying;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Vanguarr.Jellyfin.Models;

namespace Vanguarr.Jellyfin.Services;

public sealed class VanguarrSyncService : BackgroundService
{
    private readonly ILibraryManager _libraryManager;
    private readonly IUserManager _userManager;
    private readonly IPlaylistManager _playlistManager;
    private readonly ILogger<VanguarrSyncService> _logger;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public VanguarrSyncService(
        ILibraryManager libraryManager,
        IUserManager userManager,
        IPlaylistManager playlistManager,
        ILogger<VanguarrSyncService> logger)
    {
        _libraryManager = libraryManager;
        _userManager = userManager;
        _playlistManager = playlistManager;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await SyncAllUsersSafeAsync(stoppingToken).ConfigureAwait(false);

            var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();
            var delay = TimeSpan.FromMinutes(Math.Max(1, config.SyncIntervalMinutes));
            try
            {
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task SyncAllUsersSafeAsync(CancellationToken cancellationToken)
    {
        try
        {
            await SyncAllUsersAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Vanguarr playlist sync failed.");
        }
    }

    private async Task SyncAllUsersAsync(CancellationToken cancellationToken)
    {
        var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();
        if (string.IsNullOrWhiteSpace(config.VanguarrBaseUrl) || string.IsNullOrWhiteSpace(config.SuggestionsApiKey))
        {
            _logger.LogWarning("Vanguarr plugin is missing VanguarrBaseUrl or SuggestionsApiKey, skipping sync cycle.");
            return;
        }

        foreach (var user in _userManager.Users)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await SyncUserAsync(user, config, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task SyncUserAsync(User user, PluginConfiguration config, CancellationToken cancellationToken)
    {
        var response = await FetchSuggestionsAsync(user, config, cancellationToken).ConfigureAwait(false);
        if (response is null)
        {
            return;
        }

        var itemIds = ResolveLibraryItemIds(user, response.Items, config);
        await UpsertPlaylistAsync(user, itemIds, config, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation(
            "Vanguarr synced playlist for user={UserName} items={ItemCount}.",
            user.Username,
            itemIds.Count);
    }

    private async Task<VanguarrSuggestionResponse?> FetchSuggestionsAsync(
        User user,
        PluginConfiguration config,
        CancellationToken cancellationToken)
    {
        var baseUrl = config.VanguarrBaseUrl.Trim().TrimEnd('/') + "/";
        var requestUri =
            $"{baseUrl}api/jellyfin/suggestions?username={Uri.EscapeDataString(user.Username)}&user_id={Uri.EscapeDataString(user.Id.ToString())}&limit={Math.Max(1, config.SuggestionLimit)}";

        using var client = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(30),
        };
        using var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", config.SuggestionsApiKey.Trim());

        using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            _logger.LogWarning(
                "Vanguarr suggestions fetch failed for user={UserName} status={StatusCode}.",
                user.Username,
                (int)response.StatusCode);
            return null;
        }

        await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        return await JsonSerializer.DeserializeAsync<VanguarrSuggestionResponse>(
            responseStream,
            _jsonOptions,
            cancellationToken).ConfigureAwait(false);
    }

    private IReadOnlyList<Guid> ResolveLibraryItemIds(
        User user,
        IReadOnlyList<VanguarrSuggestionItem> suggestions,
        PluginConfiguration config)
    {
        var itemIds = new List<Guid>();
        foreach (var suggestion in suggestions.OrderBy(item => item.Rank).Take(Math.Max(1, config.SuggestionLimit)))
        {
            var resolved = ResolveSuggestion(user, suggestion);
            if (resolved is null || itemIds.Contains(resolved.Id))
            {
                continue;
            }

            itemIds.Add(resolved.Id);
        }

        return itemIds;
    }

    private BaseItem? ResolveSuggestion(User user, VanguarrSuggestionItem suggestion)
    {
        var providerLookup = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (!string.IsNullOrWhiteSpace(suggestion.ExternalIds.Tmdb))
        {
            providerLookup["Tmdb"] = suggestion.ExternalIds.Tmdb;
        }

        if (!string.IsNullOrWhiteSpace(suggestion.ExternalIds.Tvdb))
        {
            providerLookup["Tvdb"] = suggestion.ExternalIds.Tvdb;
        }

        if (!string.IsNullOrWhiteSpace(suggestion.ExternalIds.Imdb))
        {
            providerLookup["Imdb"] = suggestion.ExternalIds.Imdb;
        }

        var query = new InternalItemsQuery
        {
            Recursive = true,
            User = user,
            SearchTerm = suggestion.Title,
            IncludeItemTypes = suggestion.MediaType == "tv"
                ? [BaseItemKind.Series]
                : [BaseItemKind.Movie],
            Limit = 8,
            HasAnyProviderId = providerLookup,
        };

        var candidates = _libraryManager.GetItemList(query);
        return candidates.FirstOrDefault(item => MatchesSuggestion(item, suggestion))
            ?? candidates.FirstOrDefault();
    }

    private static bool MatchesSuggestion(BaseItem item, VanguarrSuggestionItem suggestion)
    {
        if (!string.IsNullOrWhiteSpace(suggestion.ExternalIds.Tmdb)
            && item.ProviderIds.TryGetValue("Tmdb", out var tmdb)
            && string.Equals(tmdb, suggestion.ExternalIds.Tmdb, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(suggestion.ExternalIds.Tvdb)
            && item.ProviderIds.TryGetValue("Tvdb", out var tvdb)
            && string.Equals(tvdb, suggestion.ExternalIds.Tvdb, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(suggestion.ExternalIds.Imdb)
            && item.ProviderIds.TryGetValue("Imdb", out var imdb)
            && string.Equals(imdb, suggestion.ExternalIds.Imdb, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var titleMatches = string.Equals(item.Name, suggestion.Title, StringComparison.OrdinalIgnoreCase);
        var yearMatches = !suggestion.ProductionYear.HasValue || item.ProductionYear == suggestion.ProductionYear;
        return titleMatches && yearMatches;
    }

    private async Task UpsertPlaylistAsync(
        User user,
        IReadOnlyList<Guid> itemIds,
        PluginConfiguration config,
        CancellationToken cancellationToken)
    {
        var existing = _playlistManager
            .GetPlaylists(user.Id)
            .FirstOrDefault(item => string.Equals(item.Name, config.PlaylistName, StringComparison.OrdinalIgnoreCase));

        if (existing is null)
        {
            if (itemIds.Count == 0)
            {
                return;
            }

            await _playlistManager.CreatePlaylist(new PlaylistCreationRequest
            {
                Name = config.PlaylistName,
                ItemIdList = itemIds,
                MediaType = MediaType.Video,
                UserId = user.Id,
                Public = false,
            }).ConfigureAwait(false);
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        await _playlistManager.UpdatePlaylist(new PlaylistUpdateRequest
        {
            Id = existing.Id,
            UserId = user.Id,
            Name = config.PlaylistName,
            Ids = itemIds,
            Public = false,
        }).ConfigureAwait(false);
    }
}
