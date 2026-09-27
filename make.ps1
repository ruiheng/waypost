param(
    [Parameter(Position = 0)]
    [string]$Target = "help",

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

$ErrorActionPreference = "Stop"

$go = if ($env:GO) { $env:GO } else { "go" }
$cmdPath = if ($env:CMD_PATH) { $env:CMD_PATH } else { "./cmd/waypost" }
$launcherCmdPath = if ($env:LAUNCHER_CMD_PATH) { $env:LAUNCHER_CMD_PATH } else { "./cmd/waypost-launcher" }
$binDir = if ($env:BIN_DIR) { $env:BIN_DIR } else { "bin" }
$binaryName = if ($env:BINARY_NAME) { $env:BINARY_NAME } else { "waypost.exe" }
$prefix = if ($env:PREFIX) { $env:PREFIX } else { Join-Path $env:USERPROFILE ".local" }
$destDir = if ($env:DESTDIR) { $env:DESTDIR } else { "" }
$installDirWasExplicit = -not [string]::IsNullOrWhiteSpace($env:INSTALL_DIR)
$installDir = if ($installDirWasExplicit) { $env:INSTALL_DIR } else { Join-Path $prefix "bin" }
$installPrefix = Split-Path -Path $installDir -Parent
if ([string]::IsNullOrWhiteSpace($installPrefix)) {
    $installPrefix = if ($installDirWasExplicit) { "." } else { $prefix }
}
$appRoot = Join-Path (Join-Path $installPrefix "lib") "waypost"
$buildOutput = Join-Path $binDir $binaryName
$runArgs = if ($env:ARGS) { $null } else { $RemainingArgs }

function Ensure-Directory {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Invoke-Go {
    param([string[]]$Arguments)

    & $go @Arguments
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Initialize-GoCache {
    $cacheRoot = Join-Path $env:LOCALAPPDATA "waypost\go"

    if ([string]::IsNullOrWhiteSpace($env:GOCACHE)) {
        $env:GOCACHE = Join-Path $cacheRoot "build"
    }
    if ([string]::IsNullOrWhiteSpace($env:GOMODCACHE)) {
        $env:GOMODCACHE = Join-Path $cacheRoot "mod"
    }

    Ensure-Directory $env:GOCACHE
    Ensure-Directory $env:GOMODCACHE
}

function Assert-CgoRequired {
    $cgoEnabled = if ($env:CGO_ENABLED) {
        $env:CGO_ENABLED
    } else {
        (& $go env CGO_ENABLED).Trim()
    }

    if ($cgoEnabled -eq "0") {
        throw @'
This project requires CGO because it uses github.com/mattn/go-sqlite3.

Current environment:
  CGO_ENABLED=0

On Windows, install a C toolchain first, then rerun with CGO enabled.
Example:
  $env:CGO_ENABLED = 1
  $env:CC = "C:/msys64/ucrt64/bin/gcc.exe"
  $env:CXX = "C:/msys64/ucrt64/bin/g++.exe"
  ./make.ps1 build
'@
    }
}

function Resolve-CompilerPath {
    param([string]$Compiler)

    if ([string]::IsNullOrWhiteSpace($Compiler)) {
        return $Compiler
    }

    if ([System.IO.Path]::IsPathRooted($Compiler) -and (Test-Path -LiteralPath $Compiler)) {
        return (Resolve-Path -LiteralPath $Compiler).Path
    }

    $resolvedCommand = Get-Command $Compiler -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $resolvedCommand -and -not [string]::IsNullOrWhiteSpace($resolvedCommand.Source)) {
        return $resolvedCommand.Source
    }

    $compilerLeaf = [System.IO.Path]::GetFileName($Compiler)
    if ([string]::IsNullOrWhiteSpace([System.IO.Path]::GetExtension($compilerLeaf))) {
        $compilerLeaf = "$compilerLeaf.exe"
    }

    $msys2Candidates = @(
        (Join-Path "C:\msys64\ucrt64\bin" $compilerLeaf),
        (Join-Path "C:\msys64\mingw64\bin" $compilerLeaf)
    )

    foreach ($candidate in $msys2Candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }

    return $Compiler
}

function Add-DirectoryToPath {
    param([string]$Directory)

    if ([string]::IsNullOrWhiteSpace($Directory) -or -not (Test-Path -LiteralPath $Directory)) {
        return
    }

    $pathParts = $env:Path -split ';'
    if ($pathParts -notcontains $Directory) {
        $env:Path = "$Directory;$env:Path"
    }
}

function Initialize-CgoToolchain {
    $configuredCC = if ($env:CC) {
        $env:CC
    } else {
        (& $go env CC).Trim()
    }
    $configuredCXX = if ($env:CXX) {
        $env:CXX
    } else {
        (& $go env CXX).Trim()
    }

    if (-not [string]::IsNullOrWhiteSpace($configuredCC)) {
        $resolvedCC = Resolve-CompilerPath -Compiler $configuredCC
        $env:CC = $resolvedCC
        Add-DirectoryToPath -Directory (Split-Path -Path $resolvedCC -Parent)
    }

    if (-not [string]::IsNullOrWhiteSpace($configuredCXX)) {
        $resolvedCXX = Resolve-CompilerPath -Compiler $configuredCXX
        $env:CXX = $resolvedCXX
        Add-DirectoryToPath -Directory (Split-Path -Path $resolvedCXX -Parent)
    }
}

function Resolve-InstallDestinationRoot {
    param(
        [string]$InstallDir,
        [string]$DestDir
    )

    if ([string]::IsNullOrWhiteSpace($DestDir)) {
        return $InstallDir
    }

    if (-not [System.IO.Path]::IsPathRooted($InstallDir)) {
        return Join-Path $DestDir $InstallDir
    }

    $qualifier = Split-Path -Path $InstallDir -Qualifier
    $relativeInstallDir = $InstallDir
    if (-not [string]::IsNullOrWhiteSpace($qualifier)) {
        $relativeInstallDir = $InstallDir.Substring($qualifier.Length)
    }

    $relativeInstallDir = $relativeInstallDir.TrimStart('\', '/')
    if ([string]::IsNullOrWhiteSpace($relativeInstallDir)) {
        return $DestDir
    }

    return Join-Path $DestDir $relativeInstallDir
}

function New-InstallVersion {
    if ($env:VERSION) {
        return $env:VERSION
    }

    $timestamp = Get-Date -Format "yyyyMMddHHmmssfff"
    $commit = (& git rev-parse --short HEAD 2>$null)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($commit)) {
        return $timestamp
    }

    $version = "$timestamp-$($commit.Trim())"
    $dirty = (& git status --porcelain 2>$null)
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($dirty)) {
        $version = "$version-dirty"
    }
    return $version
}

function Resolve-InstallVersion {
    param(
        [string]$RequestedVersion,
        [string]$VersionsRoot,
        [bool]$AllowSuffix
    )

    $version = $RequestedVersion
    $versionRoot = Join-Path $VersionsRoot $version
    if (-not $AllowSuffix -or -not (Test-Path -LiteralPath $versionRoot)) {
        return $version
    }

    $timestamp = Get-Date -Format "yyyyMMddHHmmssfff"
    for ($attempt = 1; $attempt -le 100; $attempt++) {
        $candidate = "$RequestedVersion-$timestamp-$attempt"
        $candidateRoot = Join-Path $VersionsRoot $candidate
        if (-not (Test-Path -LiteralPath $candidateRoot)) {
            return $candidate
        }
    }

    throw "Could not find an unused install version directory under '$VersionsRoot'."
}

function Move-FileReplacing {
    param(
        [string]$Source,
        [string]$Destination
    )

    if (Test-Path -LiteralPath $Destination) {
        $backup = "$Destination.bak-$PID"
        [System.IO.File]::Replace($Source, $Destination, $backup, $true)
        if (Test-Path -LiteralPath $backup) {
            Remove-Item -LiteralPath $backup -Force
        }
        return
    }
    [System.IO.File]::Move($Source, $Destination)
}

function Copy-FileReplacing {
    param(
        [string]$Source,
        [string]$Destination
    )

    $tempPath = "$Destination.tmp-$PID"
    Copy-Item -LiteralPath $Source -Destination $tempPath -Force
    Move-FileReplacing -Source $tempPath -Destination $Destination
}

function Write-ActiveVersionManifest {
    param(
        [string]$ManifestPath,
        [string]$Version,
        [string]$Executable
    )

    Ensure-Directory (Split-Path -Path $ManifestPath -Parent)
    $manifest = [ordered]@{
        version = $Version
        executable = $Executable
    }
    $tempPath = "$ManifestPath.tmp-$PID"
    $json = $manifest | ConvertTo-Json
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($tempPath, $json, $utf8NoBom)
    Move-FileReplacing -Source $tempPath -Destination $ManifestPath
}

function Install-Launcher {
    param(
        [string]$LauncherOutput,
        [string]$Destination
    )

    if (-not (Test-Path -LiteralPath $Destination)) {
        Copy-Item -LiteralPath $LauncherOutput -Destination $Destination
        return
    }

    # Windows upgrades are a hard cut: activating a new child behind an
    # older launcher would mix launcher and child protocols. A running
    # process locks its mapped executable against overwrite but not against
    # rename, so a locked launcher is moved aside and the new build still
    # takes the stable path; running processes keep their mapped copy.
    try {
        Copy-Item -LiteralPath $LauncherOutput -Destination $Destination -Force -ErrorAction Stop
        return
    } catch {
        $overwriteError = $_.Exception.Message
    }

    # Stage the new launcher under a scratch name first so the only steps
    # after the locked file moves aside are same-directory renames; a stale
    # destination can then be rolled back without a copy in between.
    $staged = "$Destination.new-$PID"
    $retired = "$Destination.retired-$PID"
    try {
        Copy-Item -LiteralPath $LauncherOutput -Destination $staged -ErrorAction Stop
        Move-Item -LiteralPath $Destination -Destination $retired -ErrorAction Stop
        Move-Item -LiteralPath $staged -Destination $Destination -ErrorAction Stop
    } catch {
        if (-not (Test-Path -LiteralPath $Destination) -and (Test-Path -LiteralPath $retired)) {
            try { Move-Item -LiteralPath $retired -Destination $Destination -ErrorAction Stop } catch {}
        }
        throw "Could not replace locked launcher '$Destination'. Stop running Waypost and Codex processes, then rerun install. No new version was activated. Original error: $overwriteError; retire error: $($_.Exception.Message)"
    } finally {
        Remove-Item -LiteralPath $staged -Force -ErrorAction SilentlyContinue
    }
}

function Remove-RetiredLaunchers {
    param(
        [string]$Directory
    )

    if (-not (Test-Path -LiteralPath $Directory)) {
        return
    }

    Get-ChildItem -LiteralPath $Directory -Filter "*.retired-*" -File -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            Remove-Item -LiteralPath $_.FullName -Force -ErrorAction Stop
        } catch {
            Write-Verbose "Retired launcher '$($_.FullName)' is still in use; leaving it for a later install."
        }
    }
}

function Remove-OldVersions {
    param(
        [string]$VersionsRoot,
        [string]$ActiveVersion
    )

    if (-not (Test-Path -LiteralPath $VersionsRoot)) {
        return
    }

    $oldVersions = Get-ChildItem -LiteralPath $VersionsRoot -Directory | Where-Object { $_.Name -ne $ActiveVersion }
    foreach ($versionDir in $oldVersions) {
        try {
            Remove-Item -LiteralPath $versionDir.FullName -Recurse -Force -ErrorAction Stop
        } catch {
            Write-Warning "Could not remove old version '$($versionDir.FullName)': $($_.Exception.Message)"
        }
    }
}

function Split-ArgumentString {
    param([string]$ArgumentString)

    if ([string]::IsNullOrWhiteSpace($ArgumentString)) {
        return @()
    }

    if (-not ("CommandLineArgumentSplitter" -as [type])) {
        Add-Type -TypeDefinition @"
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

public static class CommandLineArgumentSplitter {
    [DllImport("shell32.dll", SetLastError = true)]
    private static extern IntPtr CommandLineToArgvW(
        [MarshalAs(UnmanagedType.LPWStr)] string lpCmdLine,
        out int pNumArgs);

    [DllImport("kernel32.dll")]
    private static extern IntPtr LocalFree(IntPtr hMem);

    public static string[] Split(string commandLine) {
        IntPtr argv = CommandLineToArgvW(commandLine, out int argc);
        if (argv == IntPtr.Zero) {
            throw new Win32Exception(Marshal.GetLastWin32Error());
        }

        try {
            string[] args = new string[argc];
            for (int i = 0; i < argc; i++) {
                IntPtr argPtr = Marshal.ReadIntPtr(argv, i * IntPtr.Size);
                args[i] = Marshal.PtrToStringUni(argPtr);
            }
            return args;
        } finally {
            LocalFree(argv);
        }
    }
}
"@
    }

    return [CommandLineArgumentSplitter]::Split($ArgumentString)
}

function Show-Help {
    @(
        "Available targets:"
        "  ./make.ps1 build              Build the waypost CLI into $buildOutput"
        "  ./make.ps1 test               Run the Go test suite"
        "  ./make.ps1 run -- <args>      Run the CLI with go run and pass args through"
        "  ./make.ps1 run-mcp            Run the built-in stdio MCP server with go run"
        "  ./make.ps1 install            Install launcher into $installDir and versioned CLI into $appRoot"
        "                                 Running processes keep their version; new launches get the update"
        "  ./make.ps1 clean              Remove local build output"
    ) | ForEach-Object { Write-Output $_ }
}

switch ($Target) {
    "help" {
        Show-Help
    }
    "build" {
        Initialize-GoCache
        Assert-CgoRequired
        Initialize-CgoToolchain
        Ensure-Directory $binDir
        Invoke-Go @("build", "-o", $buildOutput, $cmdPath)
    }
    "test" {
        Initialize-GoCache
        Assert-CgoRequired
        Initialize-CgoToolchain
        Invoke-Go @("test", "./...")
    }
    "run" {
        Initialize-GoCache
        Assert-CgoRequired
        Initialize-CgoToolchain
        if ($env:ARGS) {
            $runArgs = Split-ArgumentString -ArgumentString $env:ARGS
        }
        $goArgs = @("run", $cmdPath) + $runArgs
        Invoke-Go $goArgs
    }
    "run-mcp" {
        Initialize-GoCache
        Assert-CgoRequired
        Initialize-CgoToolchain
        Invoke-Go @("run", $cmdPath, "mcp")
    }
    "install" {
        $destinationRoot = Resolve-InstallDestinationRoot -InstallDir $installDir -DestDir $destDir
        $destinationAppRoot = Resolve-InstallDestinationRoot -InstallDir $appRoot -DestDir $destDir
        $requestedVersion = New-InstallVersion
        $versionWasExplicit = -not [string]::IsNullOrWhiteSpace($env:VERSION)
        $versionsRoot = Join-Path $destinationAppRoot "versions"
        $version = Resolve-InstallVersion -RequestedVersion $requestedVersion -VersionsRoot $versionsRoot -AllowSuffix (-not $versionWasExplicit)
        $versionRoot = Join-Path $versionsRoot $version
        $versionedBinary = Join-Path $versionRoot $binaryName
        $manifestPath = Join-Path $destinationAppRoot "active-version.json"
        $launcherDestination = Join-Path $destinationRoot $binaryName
        $launcherBuildOutput = Join-Path $binDir "waypost-launcher.exe"
        $cliBuildOutput = Join-Path $binDir "waypost-install-$PID.exe"
        $manifestExecutable = Join-Path (Join-Path "versions" $version) $binaryName
        Ensure-Directory $destinationRoot
        Ensure-Directory $versionRoot
        Ensure-Directory $binDir

        Initialize-GoCache
        Assert-CgoRequired
        Initialize-CgoToolchain
        try {
            Invoke-Go @("build", "-o", $cliBuildOutput, $cmdPath)
            Invoke-Go @("build", "-o", $launcherBuildOutput, $launcherCmdPath)
            Copy-FileReplacing -Source $cliBuildOutput -Destination $versionedBinary
            Install-Launcher -LauncherOutput $launcherBuildOutput -Destination $launcherDestination
            Remove-RetiredLaunchers -Directory $destinationRoot
            Write-ActiveVersionManifest -ManifestPath $manifestPath -Version $version -Executable $manifestExecutable
        } finally {
            Remove-Item -LiteralPath $cliBuildOutput -Force -ErrorAction SilentlyContinue
        }

        if ([string]::IsNullOrWhiteSpace($destDir)) {
            Remove-OldVersions -VersionsRoot $versionsRoot -ActiveVersion $version
        }

        Write-Output "Installed $launcherDestination"
        Write-Output "Activated version $version"
    }
    "clean" {
        if (Test-Path -LiteralPath $binDir) {
            Remove-Item -LiteralPath $binDir -Recurse -Force
        }
    }
    default {
        Write-Error "Unknown target '$Target'. Run ./make.ps1 help."
    }
}
