using MediaBrowser.Controller;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Plugins;
using Microsoft.Extensions.DependencyInjection;
using Vanguarr.Jellyfin.Services;

namespace Vanguarr.Jellyfin;

public sealed class PluginServiceRegistrator : IPluginServiceRegistrator
{
    public void RegisterServices(IServiceCollection serviceCollection, IServerApplicationHost applicationHost)
    {
        serviceCollection.AddSingleton<VanguarrSuggestionCatalogService>();

        var userManager = applicationHost.Resolve<IUserManager>();
        if (userManager is null)
        {
            return;
        }

        foreach (var user in userManager.Users)
        {
            var userId = user.Id;
            var userName = user.Username;
            serviceCollection.AddSingleton<MediaBrowser.Controller.Channels.IChannel>(serviceProvider =>
                new VanguarrSuggestedChannel(
                    userId,
                    userName,
                    serviceProvider.GetRequiredService<IUserManager>(),
                    serviceProvider.GetRequiredService<VanguarrSuggestionCatalogService>(),
                    serviceProvider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<VanguarrSuggestedChannel>>()));
        }
    }
}
