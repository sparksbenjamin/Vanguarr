using System.Text.Json.Serialization;
using Jellyfin.Database.Implementations.Entities;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Model.Querying;
using MediaBrowser.Model.IO;
using MediaBrowser.Controller.Providers;
using Vanguarr.Jellyfin.Services;

namespace Vanguarr.Jellyfin.Folders;

public abstract class VanguarrSuggestedLibraryFolder : BasePluginFolder
{
    [JsonIgnore]
    public override bool IsHidden => false;

    [JsonIgnore]
    public override bool IsPreSorted => true;

    [JsonIgnore]
    public override bool SupportsInheritedParentImages => false;

    [JsonIgnore]
    public override bool SupportsPlayedStatus => false;

    protected abstract string SuggestedMediaType { get; }

    public override string GetClientTypeName()
    {
        return "CollectionFolder";
    }

    protected override QueryResult<BaseItem> GetItemsInternal(InternalItemsQuery query)
    {
        var user = query.User;
        var catalogService = VanguarrSuggestionCatalogService.Current;
        if (user is null || catalogService is null)
        {
            return new QueryResult<BaseItem>
            {
                Items = [],
                TotalRecordCount = 0,
            };
        }

        var resolvedSuggestions = catalogService.GetResolvedSuggestionsAsync(
                user,
                SuggestedMediaType,
                CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        IEnumerable<BaseItem> items = resolvedSuggestions.Select(item => item.Item);
        if (!string.IsNullOrWhiteSpace(query.SearchTerm))
        {
            items = items.Where(item => item.Name.Contains(query.SearchTerm, StringComparison.OrdinalIgnoreCase));
        }

        var totalRecordCount = items.Count();

        if (query.StartIndex.HasValue)
        {
            items = items.Skip(query.StartIndex.Value);
        }

        if (query.Limit.HasValue)
        {
            items = items.Take(query.Limit.Value);
        }

        return new QueryResult<BaseItem>
        {
            Items = items.ToArray(),
            TotalRecordCount = totalRecordCount,
        };
    }

    protected override Task ValidateChildrenInternal(
        IProgress<double> progress,
        bool recursive,
        bool refreshChildMetadata,
        bool allowRemoveRoot,
        MetadataRefreshOptions refreshOptions,
        IDirectoryService directoryService,
        CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
