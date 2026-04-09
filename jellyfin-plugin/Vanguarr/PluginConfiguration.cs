using MediaBrowser.Model.Plugins;

namespace Vanguarr.Jellyfin;

public class PluginConfiguration : BasePluginConfiguration
{
    public string VanguarrBaseUrl { get; set; } = "http://vanguarr:8000";

    public string SuggestionsApiKey { get; set; } = string.Empty;

    public int SyncIntervalMinutes { get; set; } = 15;

    public int SuggestionLimit { get; set; } = 20;

    public string PlaylistName { get; set; } = "Suggested for You";
}
