using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Jellyfin.Database.Implementations.Entities;
using MediaBrowser.Controller.Channels;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Channels;
using MediaBrowser.Model.Dto;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;

namespace Vanguarr.Jellyfin.Services;

public sealed class VanguarrSuggestedChannel : IChannel, ISupportsLatestMedia, IHasCacheKey
{
    private const string MoviePrefix = "movie";
    private const string SeriesPrefix = "series";
    private const string SeasonPrefix = "season";
    private const string EpisodePrefix = "episode";

    private readonly IUserManager _userManager;
    private readonly VanguarrSuggestionCatalogService _catalogService;
    private readonly ILogger<VanguarrSuggestedChannel> _logger;
    private readonly string _channelName;

    public VanguarrSuggestedChannel(
        IUserManager userManager,
        VanguarrSuggestionCatalogService catalogService,
        ILogger<VanguarrSuggestedChannel> logger)
    {
        _userManager = userManager;
        _catalogService = catalogService;
        _logger = logger;
        _channelName = GetConfiguredChannelLabel();
    }

    public string Name => _channelName;

    public string Description => "Personalized Vanguarr suggestions for the active Jellyfin user.";

    public string DataVersion
    {
        get
        {
            var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();
            var refreshInterval = Math.Max(1, config.SyncIntervalMinutes);
            var refreshBucket = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 60 / refreshInterval;
            return $"2:{refreshInterval}:{Math.Max(1, config.SuggestionLimit)}:{refreshBucket}:{BuildConfigSignature(config)}";
        }
    }

    public string HomePageUrl => string.Empty;

    public ChannelParentalRating ParentalRating => ChannelParentalRating.GeneralAudience;

    public InternalChannelFeatures GetChannelFeatures()
    {
        return new InternalChannelFeatures
        {
            MediaTypes = [ChannelMediaType.Video],
            ContentTypes =
            [
                ChannelMediaContentType.Movie,
                ChannelMediaContentType.Episode,
            ],
            MaxPageSize = Math.Max(1, Plugin.Instance?.Configuration?.SuggestionLimit ?? 20),
            SupportsSortOrderToggle = false,
        };
    }

    public bool IsEnabledFor(string userId) => TryGetUser(userId) is not null;

    public string? GetCacheKey(string? userId)
    {
        return TryNormalizeUserId(userId, out var normalizedUserId)
            ? "-user-" + normalizedUserId.ToString("N", CultureInfo.InvariantCulture)
            : "-anonymous";
    }

    public async Task<ChannelItemResult> GetChannelItems(InternalChannelItemQuery query, CancellationToken cancellationToken)
    {
        var user = TryGetUser(query.UserId);
        if (user is null)
        {
            _logger.LogDebug(
                "Skipping Vanguarr channel request without a concrete Jellyfin user context. queryUserId={UserId}",
                query.UserId);
            return new ChannelItemResult();
        }

        var items = string.IsNullOrWhiteSpace(query.FolderId)
            ? await GetRootItemsAsync(user, cancellationToken).ConfigureAwait(false)
            : GetFolderItems(user, query.FolderId);

        var pagedItems = items
            .Skip(query.StartIndex ?? 0)
            .Take(query.Limit ?? items.Count)
            .ToList();

        return new ChannelItemResult
        {
            Items = pagedItems,
            TotalRecordCount = items.Count,
        };
    }

    public async Task<IEnumerable<ChannelItemInfo>> GetLatestMedia(ChannelLatestMediaSearch request, CancellationToken cancellationToken)
    {
        var user = TryGetUser(request.UserId);
        if (user is null)
        {
            return [];
        }

        return await GetRootItemsAsync(user, cancellationToken).ConfigureAwait(false);
    }

    public Task<DynamicImageResponse> GetChannelImage(ImageType type, CancellationToken cancellationToken)
    {
        return Task.FromResult(new DynamicImageResponse
        {
            HasImage = false,
        });
    }

    public IEnumerable<ImageType> GetSupportedChannelImages()
    {
        return [];
    }

    private async Task<List<ChannelItemInfo>> GetRootItemsAsync(User user, CancellationToken cancellationToken)
    {
        var suggestions = await _catalogService.GetResolvedSuggestionsAsync(user, cancellationToken).ConfigureAwait(false);
        return suggestions
            .Select(suggestion => MapSuggestion(user, suggestion))
            .Where(item => item is not null)
            .Cast<ChannelItemInfo>()
            .ToList();
    }

    private List<ChannelItemInfo> GetFolderItems(User user, string folderId)
    {
        var parentItem = ResolveExternalItem(user, folderId);
        if (parentItem is null)
        {
            return [];
        }

        return _catalogService.GetBrowseChildren(user, parentItem)
            .Select(child => MapLibraryItem(user, child))
            .Where(item => item is not null)
            .Cast<ChannelItemInfo>()
            .ToList();
    }

    private ChannelItemInfo? MapSuggestion(User user, ResolvedVanguarrSuggestion suggestion)
    {
        return MapLibraryItem(user, suggestion.Item);
    }

    private ChannelItemInfo? MapLibraryItem(User user, BaseItem item)
    {
        if (item is Series)
        {
            return BuildFolderItem(user, item, SeriesPrefix);
        }

        if (item is Season)
        {
            return BuildFolderItem(user, item, SeasonPrefix);
        }

        return BuildMediaItem(user, item);
    }

    private ChannelItemInfo BuildFolderItem(User user, BaseItem item, string prefix)
    {
        var channelItem = CreateCommonChannelItem(item, BuildExternalId(user, prefix, item.Id));
        channelItem.Type = ChannelItemType.Folder;
        channelItem.FolderType = ChannelFolderType.Container;
        channelItem.MediaType = ChannelMediaType.Video;
        return channelItem;
    }

    private ChannelItemInfo BuildMediaItem(User user, BaseItem item)
    {
        var prefix = item is Episode ? EpisodePrefix : MoviePrefix;
        var contentType = item is Episode ? ChannelMediaContentType.Episode : ChannelMediaContentType.Movie;

        var channelItem = CreateCommonChannelItem(item, BuildExternalId(user, prefix, item.Id));
        channelItem.Type = ChannelItemType.Media;
        channelItem.MediaType = ChannelMediaType.Video;
        channelItem.ContentType = contentType;
        channelItem.MediaSources = _catalogService.GetMediaSources(item, user).ToList();
        return channelItem;
    }

    private static ChannelItemInfo CreateCommonChannelItem(BaseItem item, string externalId)
    {
        return new ChannelItemInfo
        {
            Id = externalId,
            Name = item.Name,
            SeriesName = item is Episode episode ? episode.SeriesName : string.Empty,
            Overview = item.Overview,
            OfficialRating = item.OfficialRating,
            CommunityRating = item.CommunityRating,
            RunTimeTicks = item.RunTimeTicks,
            ProductionYear = item.ProductionYear,
            PremiereDate = item.PremiereDate,
            DateCreated = item.DateCreated,
            DateModified = item.DateModified,
            OriginalTitle = item.OriginalTitle,
            IndexNumber = item.IndexNumber,
            ParentIndexNumber = item.ParentIndexNumber,
            ProviderIds = item.ProviderIds is null
                ? []
                : new Dictionary<string, string>(item.ProviderIds, StringComparer.OrdinalIgnoreCase),
            Genres = item.Genres?.ToList() ?? [],
            Tags = item.Tags?.ToList() ?? [],
            ImageUrl = string.IsNullOrWhiteSpace(item.PrimaryImagePath) ? string.Empty : item.PrimaryImagePath,
        };
    }

    private BaseItem? ResolveExternalItem(User user, string externalId)
    {
        if (!TryParseExternalId(externalId, out var ownerUserId, out var itemId))
        {
            return null;
        }

        if (ownerUserId != user.Id)
        {
            return null;
        }

        return _catalogService.GetLibraryItem(itemId);
    }

    private static string BuildExternalId(User user, string prefix, Guid itemId)
    {
        return $"{prefix}:{user.Id:N}:{itemId:N}";
    }

    private static bool TryParseExternalId(string? externalId, out Guid userId, out Guid itemId)
    {
        userId = Guid.Empty;
        itemId = Guid.Empty;
        if (string.IsNullOrWhiteSpace(externalId))
        {
            return false;
        }

        var parts = externalId.Split(':', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 3)
        {
            return false;
        }

        return TryNormalizeUserId(parts[1], out userId)
            && (Guid.TryParseExact(parts[2], "N", out itemId) || Guid.TryParse(parts[2], out itemId));
    }

    private static string GetConfiguredChannelLabel()
    {
        var configuredName = Plugin.Instance?.Configuration?.PlaylistName?.Trim();
        return string.IsNullOrWhiteSpace(configuredName) ? "Suggested for You" : configuredName;
    }

    private static string BuildConfigSignature(PluginConfiguration config)
    {
        var signatureSource = string.Join(
            "|",
            config.VanguarrBaseUrl?.Trim() ?? string.Empty,
            config.SuggestionsApiKey?.Trim() ?? string.Empty,
            config.PlaylistName?.Trim() ?? string.Empty);
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(signatureSource));
        return Convert.ToHexString(hash[..6]);
    }

    private User? TryGetUser(string? userId)
    {
        return TryNormalizeUserId(userId, out var normalizedUserId)
            ? _userManager.GetUserById(normalizedUserId)
            : null;
    }

    private User? TryGetUser(Guid userId)
    {
        return userId == Guid.Empty ? null : _userManager.GetUserById(userId);
    }

    private static bool TryNormalizeUserId(string? userId, out Guid normalizedUserId)
    {
        normalizedUserId = Guid.Empty;
        if (string.IsNullOrWhiteSpace(userId))
        {
            return false;
        }

        return Guid.TryParse(userId, out normalizedUserId)
            || Guid.TryParseExact(userId, "N", out normalizedUserId);
    }
}
