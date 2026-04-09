using MediaBrowser.Model.Tasks;
using Microsoft.Extensions.Logging;

namespace Vanguarr.Jellyfin.Services;

public sealed class VanguarrStartupService : IScheduledTask
{
    private readonly VanguarrSuggestedViewsRegistrar _registrar;
    private readonly ILogger<VanguarrStartupService> _logger;

    public VanguarrStartupService(
        VanguarrSuggestedViewsRegistrar registrar,
        ILogger<VanguarrStartupService> logger)
    {
        _registrar = registrar;
        _logger = logger;
    }

    public string Name => "Vanguarr Startup";

    public string Key => "Vanguarr.Jellyfin.Startup";

    public string Description => "Registers Vanguarr's native Suggested Movies and Suggested Shows views after Jellyfin startup.";

    public string Category => "Startup Services";

    public async Task ExecuteAsync(IProgress<double> progress, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Vanguarr startup service running.");
        await _registrar.EnsureSuggestedViewsAsync(cancellationToken).ConfigureAwait(false);
    }

    public IEnumerable<TaskTriggerInfo> GetDefaultTriggers()
    {
        yield return new TaskTriggerInfo
        {
            Type = TaskTriggerInfoType.StartupTrigger,
        };
    }
}
