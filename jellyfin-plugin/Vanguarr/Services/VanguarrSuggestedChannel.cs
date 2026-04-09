using System.Globalization;
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

public sealed class VanguarrSuggestedChannel : IChannel, ISupportsLatestMedia
{
    private const string MoviePrefix = "movie";
    private const string SeriesPrefix = "series";
    private const string SeasonPrefix = "season";
    private const string EpisodePrefix = "episode";

    private readonly Guid _jellyfinUserId;
    private readonly string _jellyfinUserName;
    private readonly string _channelName;
    private readonly IUserManager _userManager;
    private readonly VanguarrSuggestionCatalogService _catalogService;
    private readonly ILogger<VanguarrSuggestedChannel> _logger;

    public VanguarrSuggestedChannel(
        Guid jellyfinUserId,
        string jellyfinUserName,
        IUserManager userManager,
        VanguarrSuggestionCatalogService catalogService,
        ILogger<VanguarrSuggestedChannel> logger)
    {
        _jellyfinUserId = jellyfinUserId;
        _jellyfinUserName = string.IsNullOrWhiteSpace(jellyfinUserName) ? jellyfinUserId.ToString("N", CultureInfo.InvariantCulture) : jellyfinUserName;
        _channelName = $"{GetConfiguredChannelLabel()} - {_jellyfinUserName}";
        _userManager = userManager;
        _catalogService = catalogService;
        _logger = logger;
    }

    public string Name => _channelName;

    public string Description => $"Personalized Vanguarr suggestions for {_jellyfinUserName}.";

    public string DataVersion
    {
        get
        {
            var config = Plugin.Instance?.Configuration ?? new PluginConfiguration();
            var refreshInterval = Math.Max(1, config.SyncIntervalMinutes);
            var refreshBucket = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 60 / refreshInterval;
            return $"{refreshInterval}:{Math.Max(1, config.SuggestionLimit)}:{refreshBucket}";
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

    public bool IsEnabledFor(string userId)
    {
        if (Guid.TryParse(userId, out var parsedGuid))
        {
            return parsedGuid == _jellyfinUserId;
        }

        return string.Equals(
            userId,
            _jellyfinUserId.ToString("N", CultureInfo.InvariantCulture),
            StringComparison.OrdinalIgnoreCase);
    }

    public async Task<ChannelItemResult> GetChannelItems(InternalChannelItemQuery query, CancellationToken cancellationToken)
    {
        if (!IsEnabledFor(query.UserId.ToString("N", CultureInfo.InvariantCulture)))
        {
            return new ChannelItemResult();
        }

        var user = _userManager.GetUserById(_jellyfinUserId);
        if (user is null)
        {
            _logger.LogWarning("Unable to load Jellyfin user {UserId} for Vanguarr channel.", _jellyfinUserId);
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
        if (!IsEnabledFor(request.UserId))
        {
            return [];
        }

        var user = _userManager.GetUserById(_jellyfinUserId);
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
        var parentItem = ResolveExternalItem(folderId);
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
            return BuildFolderItem(item, SeriesPrefix, ChannelFolderType.Series);
        }

        if (item is Season)
        {
            return BuildFolderItem(item, SeasonPrefix, ChannelFolderType.Season);
        }

        return BuildMediaItem(user, item);
    }

    private ChannelItemInfo BuildFolderItem(BaseItem item, string prefix, ChannelFolderType folderType)
    {
        var channelItem = CreateCommonChannelItem(item, BuildExternalId(prefix, item.Id));
        channelItem.Type = ChannelItemType.Folder;
        channelItem.FolderType = folderType;
        channelItem.MediaType = ChannelMediaType.Video;
        return channelItem;
    }

    private ChannelItemInfo BuildMediaItem(User user, BaseItem item)
    {
        var prefix = item is Episode ? EpisodePrefix : MoviePrefix;
        var contentType = item is Episode ? ChannelMediaContentType.Episode : ChannelMediaContentType.Movie;

        var channelItem = CreateCommonChannelItem(item, BuildExternalId(prefix, item.Id));
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

    private BaseItem? ResolveExternalItem(string externalId)
    {
        if (!TryParseExternalId(externalId, out var itemId))
        {
            return null;
        }

        return _catalogService.GetLibraryItem(itemId);
    }

    private static string BuildExternalId(string prefix, Guid itemId)
    {
        return $"{prefix}:{itemId:N}";
    }

    private static bool TryParseExternalId(string? externalId, out Guid itemId)
    {
        itemId = Guid.Empty;
        if (string.IsNullOrWhiteSpace(externalId))
        {
            return false;
        }

        var separatorIndex = externalId.IndexOf(':');
        if (separatorIndex < 0 || separatorIndex == externalId.Length - 1)
        {
            return false;
        }

        return Guid.TryParseExact(externalId[(separatorIndex + 1)..], "N", out itemId)
            || Guid.TryParse(externalId[(separatorIndex + 1)..], out itemId);
    }

    private static string GetConfiguredChannelLabel()
    {
        var configuredName = Plugin.Instance?.Configuration?.PlaylistName?.Trim();
        return string.IsNullOrWhiteSpace(configuredName) ? "Suggested for You" : configuredName;
    }
}
