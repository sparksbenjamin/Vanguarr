using MediaBrowser.Common.Configuration;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using Microsoft.Extensions.Logging;
using Vanguarr.Jellyfin.Folders;

namespace Vanguarr.Jellyfin.Services;

public sealed class VanguarrSuggestedViewsRegistrar
{
    private readonly IApplicationPaths _applicationPaths;
    private readonly ILibraryManager _libraryManager;
    private readonly ILogger<VanguarrSuggestedViewsRegistrar> _logger;

    public VanguarrSuggestedViewsRegistrar(
        IApplicationPaths applicationPaths,
        ILibraryManager libraryManager,
        ILogger<VanguarrSuggestedViewsRegistrar> logger)
    {
        _applicationPaths = applicationPaths;
        _libraryManager = libraryManager;
        _logger = logger;
    }

    public Task EnsureSuggestedViewsAsync(CancellationToken cancellationToken)
    {
        var userRoot = _libraryManager.GetUserRootFolder();
        EnsureFolder<VanguarrSuggestedMoviesFolder>(
            userRoot,
            Path.Combine(GetViewsRootPath(), "suggested-movies"),
            GetConfiguredName(
                Plugin.Instance?.Configuration?.SuggestedMoviesName,
                "Suggested Movies"));
        EnsureFolder<VanguarrSuggestedShowsFolder>(
            userRoot,
            Path.Combine(GetViewsRootPath(), "suggested-shows"),
            GetConfiguredName(
                Plugin.Instance?.Configuration?.SuggestedShowsName,
                "Suggested Shows"));

        _logger.LogInformation(
            "Ensured Vanguarr suggested views are registered under Jellyfin user root. catalogReady={CatalogReady}",
            VanguarrSuggestionCatalogService.Current is not null);

        return Task.CompletedTask;
    }

    private void EnsureFolder<TFolder>(Folder userRoot, string folderPath, string displayName)
        where TFolder : BasePluginFolder, new()
    {
        Directory.CreateDirectory(folderPath);

        var existingFolder = userRoot.Children
            .OfType<TFolder>()
            .FirstOrDefault(item => string.Equals(item.Path, folderPath, StringComparison.OrdinalIgnoreCase))
            ?? _libraryManager.GetItemById(_libraryManager.GetNewItemId(folderPath, typeof(TFolder))) as TFolder;

        if (existingFolder is null)
        {
            var info = new DirectoryInfo(folderPath);
            var folder = new TFolder();
            folder.Path = folderPath;
            folder.Name = displayName;
            folder.DateCreated = info.CreationTimeUtc;
            folder.DateModified = info.LastWriteTimeUtc;
            folder.ParentId = userRoot.Id;

            userRoot.AddChild(folder);
            _logger.LogInformation(
                "Created Vanguarr suggested view {ViewName} at {FolderPath}.",
                displayName,
                folderPath);
            return;
        }

        var requiresSave = false;
        if (!existingFolder.ParentId.Equals(userRoot.Id))
        {
            existingFolder.ParentId = userRoot.Id;
            requiresSave = true;
        }

        if (!string.Equals(existingFolder.Name, displayName, StringComparison.Ordinal))
        {
            existingFolder.Name = displayName;
            requiresSave = true;
        }

        if (!string.Equals(existingFolder.Path, folderPath, StringComparison.OrdinalIgnoreCase))
        {
            existingFolder.Path = folderPath;
            requiresSave = true;
        }

        if (requiresSave)
        {
            existingFolder.UpdateToRepositoryAsync(ItemUpdateType.MetadataImport, cancellationToken: CancellationToken.None)
                .GetAwaiter()
                .GetResult();
            _logger.LogInformation(
                "Updated Vanguarr suggested view {ViewName} at {FolderPath}.",
                displayName,
                folderPath);
        }
    }

    private string GetViewsRootPath()
    {
        return Path.Combine(_applicationPaths.DataPath, "vanguarr", "jellyfin-suggested-views");
    }

    private static string GetConfiguredName(string? configuredName, string fallback)
    {
        return string.IsNullOrWhiteSpace(configuredName) ? fallback : configuredName.Trim();
    }
}
