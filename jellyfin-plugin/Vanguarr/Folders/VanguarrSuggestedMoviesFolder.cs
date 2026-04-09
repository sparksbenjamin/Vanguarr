namespace Vanguarr.Jellyfin.Folders;

public sealed class VanguarrSuggestedMoviesFolder : VanguarrSuggestedLibraryFolder
{
    protected override string SuggestedMediaType => "movie";
}
