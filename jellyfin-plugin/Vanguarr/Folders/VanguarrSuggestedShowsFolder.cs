namespace Vanguarr.Jellyfin.Folders;

public sealed class VanguarrSuggestedShowsFolder : VanguarrSuggestedLibraryFolder
{
    protected override string SuggestedMediaType => "tv";
}
