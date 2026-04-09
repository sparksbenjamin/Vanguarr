param(
    [string]$Configuration = "Release"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectPath = Join-Path $root "Vanguarr\\Vanguarr.csproj"
$projectXml = [xml](Get-Content $projectPath)
$version = $projectXml.Project.PropertyGroup.Version
$buildOutput = Join-Path $root ("Vanguarr\\bin\\{0}\\net9.0" -f $Configuration)
$stageDir = Join-Path $root ".stage"
$distDir = Join-Path $root "dist"
$zipName = "vanguarr-$version.zip"
$zipPath = Join-Path $distDir $zipName

dotnet build $projectPath -c $Configuration | Out-Host

if (Test-Path $stageDir) {
    Remove-Item -LiteralPath $stageDir -Recurse -Force
}

New-Item -ItemType Directory -Force -Path $stageDir | Out-Null
New-Item -ItemType Directory -Force -Path $distDir | Out-Null

Copy-Item -LiteralPath (Join-Path $buildOutput "Vanguarr.dll") -Destination $stageDir

if (Test-Path $zipPath) {
    Remove-Item -LiteralPath $zipPath -Force
}

Compress-Archive -Path (Join-Path $stageDir "*") -DestinationPath $zipPath

$checksum = (Get-FileHash -LiteralPath $zipPath -Algorithm MD5).Hash.ToLowerInvariant()

Write-Host "Packaged plugin: $zipPath"
Write-Host "MD5 checksum: $checksum"
