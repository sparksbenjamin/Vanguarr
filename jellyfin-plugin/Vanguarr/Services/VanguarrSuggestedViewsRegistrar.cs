using MediaBrowser.Common.Configuration;
using MediaBrowser.Controller.Drawing;
using MediaBrowser.Controller.Dto;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.Entities;
using MediaBrowser.Model.Querying;
using Microsoft.Extensions.Logging;
using Vanguarr.Jellyfin.Folders;
using BaseItemKind = Jellyfin.Data.Enums.BaseItemKind;
using ItemSortBy = Jellyfin.Data.Enums.ItemSortBy;
using SortOrder = Jellyfin.Database.Implementations.Enums.SortOrder;

namespace Vanguarr.Jellyfin.Services;

public sealed class VanguarrSuggestedViewsRegistrar
{
    private readonly IApplicationPaths _applicationPaths;
    private readonly IImageProcessor _imageProcessor;
    private readonly ILibraryManager _libraryManager;
    private readonly VanguarrSuggestionCatalogService _catalogService;
    private readonly IProviderManager _providerManager;
    private readonly ILogger<VanguarrSuggestedViewsRegistrar> _logger;

    public VanguarrSuggestedViewsRegistrar(
        IApplicationPaths applicationPaths,
        IImageProcessor imageProcessor,
        ILibraryManager libraryManager,
        VanguarrSuggestionCatalogService catalogService,
        IProviderManager providerManager,
        ILogger<VanguarrSuggestedViewsRegistrar> logger)
    {
        _applicationPaths = applicationPaths;
        _imageProcessor = imageProcessor;
        _libraryManager = libraryManager;
        _catalogService = catalogService;
        _providerManager = providerManager;
        _logger = logger;
    }

    public Task EnsureSuggestedViewsAsync(CancellationToken cancellationToken)
    {
        var userRoot = _libraryManager.GetUserRootFolder();
        var moviesFolder = EnsureFolder<VanguarrSuggestedMoviesFolder>(
            userRoot,
            Path.Combine(GetViewsRootPath(), "suggested-movies"),
            GetConfiguredName(
                Plugin.Instance?.Configuration?.SuggestedMoviesName,
                "Suggested Movies"));
        var showsFolder = EnsureFolder<VanguarrSuggestedShowsFolder>(
            userRoot,
            Path.Combine(GetViewsRootPath(), "suggested-shows"),
            GetConfiguredName(
                Plugin.Instance?.Configuration?.SuggestedShowsName,
                "Suggested Shows"));

        EnsureFolderArtwork(moviesFolder, [BaseItemKind.Movie], cancellationToken);
        EnsureFolderArtwork(showsFolder, [BaseItemKind.Series], cancellationToken);

        _logger.LogInformation(
            "Ensured Vanguarr suggested views are registered under Jellyfin user root. catalogReady={CatalogReady}",
            _catalogService is not null && VanguarrSuggestionCatalogService.Current is not null);

        return Task.CompletedTask;
    }

    private TFolder EnsureFolder<TFolder>(Folder userRoot, string folderPath, string displayName)
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
            return folder;
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

        return existingFolder;
    }

    private void EnsureFolderArtwork(
        BasePluginFolder folder,
        BaseItemKind[] includeItemTypes,
        CancellationToken cancellationToken)
    {
        if (!_imageProcessor.SupportsImageCollageCreation)
        {
            return;
        }

        var candidateItems = _libraryManager.GetItemList(new InternalItemsQuery
        {
            Recursive = true,
            IncludeItemTypes = includeItemTypes,
            ImageTypes = [ImageType.Primary],
            DtoOptions = new DtoOptions(false),
            Limit = 8,
            OrderBy =
            [
                (ItemSortBy.Random, SortOrder.Ascending),
            ],
        });

        var inputPaths = candidateItems
            .Select(GetArtworkPath)
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Take(8)
            .ToArray();

        if (inputPaths.Length == 0)
        {
            return;
        }

        var tempDir = Path.Combine(_applicationPaths.TempDirectory, "vanguarr-images");
        Directory.CreateDirectory(tempDir);

        var outputPath = Path.Combine(
            tempDir,
            $"{folder.Name.Replace(Path.DirectorySeparatorChar, '-').Replace(Path.AltDirectorySeparatorChar, '-')}.png");

        _imageProcessor.CreateImageCollage(new ImageCollageOptions
        {
            InputPaths = inputPaths,
            OutputPath = outputPath,
            Width = 960,
            Height = 540,
        }, folder.Name);

        _providerManager.SaveImage(
                folder,
                outputPath,
                "image/png",
                ImageType.Primary,
                null,
                true,
                cancellationToken)
            .GetAwaiter()
            .GetResult();
    }

    private static string? GetArtworkPath(BaseItem item)
    {
        var backdrop = item.GetImageInfo(ImageType.Backdrop, 0);
        if (backdrop is not null && backdrop.IsLocalFile)
        {
            return backdrop.Path;
        }

        var primary = item.GetImageInfo(ImageType.Primary, 0);
        if (primary is not null && primary.IsLocalFile)
        {
            return primary.Path;
        }

        var thumb = item.GetImageInfo(ImageType.Thumb, 0);
        if (thumb is not null && thumb.IsLocalFile)
        {
            return thumb.Path;
        }

        return null;
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
