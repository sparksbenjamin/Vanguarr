using System.Net.Http.Headers;
using System.Text.Json;
using BaseItemKind = Jellyfin.Data.Enums.BaseItemKind;
using ItemSortBy = Jellyfin.Data.Enums.ItemSortBy;
using Jellyfin.Database.Implementations.Entities;
using SortOrder = Jellyfin.Database.Implementations.Enums.SortOrder;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Dto;
using Microsoft.Extensions.Logging;
using Vanguarr.Jellyfin.Models;

namespace Vanguarr.Jellyfin.Services;

public sealed class VanguarrSuggestionCatalogService
{
    private readonly ILibraryManager _libraryManager;
    private readonly IMediaSourceManager _mediaSourceManager;
    private readonly ILogger<VanguarrSuggestionCatalogService> _logger;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public VanguarrSuggestionCatalogService(
        ILibraryManager libraryManager,
        IMediaSourceManager mediaSourceManager,
        ILogger<VanguarrSuggestionCatalogService> logger)
    {
        _libraryManager = libraryManager;
        _mediaSourceManager = mediaSourceManager;
        _logger = logger;
    }

    public async Task<IReadOnlyList<ResolvedVanguarrSuggestion>> GetResolvedSuggestionsAsync(
        User user,
        CancellationToken cancellationToken)
    {
        var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();
        if (string.IsNullOrWhiteSpace(config.VanguarrBaseUrl) || string.IsNullOrWhiteSpace(config.SuggestionsApiKey))
        {
            _logger.LogWarning(
                "Vanguarr plugin is missing VanguarrBaseUrl or SuggestionsApiKey, skipping suggestions for user={UserName}.",
                user.Username);
            return [];
        }

        var response = await FetchSuggestionsAsync(user, config, cancellationToken).ConfigureAwait(false);
        if (response is null)
        {
            return [];
        }

        _logger.LogInformation(
            "Vanguarr returned {SuggestionCount} suggestion candidate(s) for user={UserName}.",
            response.Items.Count,
            user.Username);

        var resolvedSuggestions = new List<ResolvedVanguarrSuggestion>();
        foreach (var suggestion in response.Items.OrderBy(item => item.Rank).Take(Math.Max(1, config.SuggestionLimit)))
        {
            var resolvedItem = ResolveSuggestion(user, suggestion);
            if (resolvedItem is null || resolvedSuggestions.Any(item => item.Item.Id == resolvedItem.Id))
            {
                continue;
            }

            resolvedSuggestions.Add(new ResolvedVanguarrSuggestion(resolvedItem, suggestion));
        }

        if (response.Items.Count > 0 && resolvedSuggestions.Count == 0)
        {
            _logger.LogWarning(
                "Vanguarr returned suggestions for user={UserName}, but Jellyfin resolved 0 matching library items.",
                user.Username);
        }
        else
        {
            _logger.LogInformation(
                "Jellyfin resolved {ResolvedCount} suggestion item(s) for user={UserName}.",
                resolvedSuggestions.Count,
                user.Username);
        }

        return resolvedSuggestions;
    }

    public BaseItem? GetLibraryItem(Guid itemId)
    {
        return _libraryManager.GetItemById(itemId);
    }

    public IReadOnlyList<BaseItem> GetBrowseChildren(User user, BaseItem parentItem)
    {
        if (parentItem is Series)
        {
            var seasons = QueryChildren(
                user,
                parentItem.Id,
                [BaseItemKind.Season]);
            if (seasons.Count > 0)
            {
                return seasons;
            }

            return QueryChildren(
                user,
                parentItem.Id,
                [BaseItemKind.Episode]);
        }

        if (parentItem is Season)
        {
            return QueryChildren(
                user,
                parentItem.Id,
                [BaseItemKind.Episode]);
        }

        return [];
    }

    public IReadOnlyList<MediaSourceInfo> GetMediaSources(BaseItem item, User user)
    {
        return _mediaSourceManager.GetStaticMediaSources(item, false, user);
    }

    private IReadOnlyList<BaseItem> QueryChildren(
        User user,
        Guid parentId,
        BaseItemKind[] includeItemTypes)
    {
        var query = new InternalItemsQuery(user)
        {
            ParentId = parentId,
            Recursive = false,
            IncludeItemTypes = includeItemTypes,
            OrderBy =
            [
                (ItemSortBy.SortName, SortOrder.Ascending),
            ],
        };

        return _libraryManager.GetItemList(query);
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

        var query = new InternalItemsQuery(user)
        {
            Recursive = true,
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
}

public sealed record ResolvedVanguarrSuggestion(BaseItem Item, VanguarrSuggestionItem Suggestion);
