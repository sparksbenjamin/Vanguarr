using MediaBrowser.Controller;
using MediaBrowser.Controller.Plugins;
using Microsoft.Extensions.DependencyInjection;
using Vanguarr.Jellyfin.Services;

namespace Vanguarr.Jellyfin;

public sealed class PluginServiceRegistrator : IPluginServiceRegistrator
{
    public void RegisterServices(IServiceCollection serviceCollection, IServerApplicationHost applicationHost)
    {
        serviceCollection.AddSingleton<VanguarrSuggestionCatalogService>();
        serviceCollection.AddSingleton<VanguarrSuggestedViewsRegistrar>();
    }
}
