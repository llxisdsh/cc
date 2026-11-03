param(
    [string]$Go = 'go',
    [string[]]$GoArgs = @('test', '-run=^TestV28', '-count=1', '.')
)

$ErrorActionPreference = 'Stop'
if ($GoArgs.Count -eq 0 -or $GoArgs[0] -notin @('build', 'test', 'run', 'vet', 'list')) {
    throw 'GoArgs must start with build, test, run, vet, or list.'
}
$oldExperiment = $env:GOEXPERIMENT
$oldToolchain = $env:GOTOOLCHAIN
function Write-OverlayFile([string]$Path, [string]$Content) {
    if (-not [IO.File]::Exists($Path) -or [IO.File]::ReadAllText($Path) -ne $Content) {
        [IO.File]::WriteAllText($Path, $Content, [Text.UTF8Encoding]::new($false))
    }
}
try {
    $experiments = @($oldExperiment -split ',' | Where-Object { $_ -and $_ -notin @('simd', 'nosimd') })
    $env:GOEXPERIMENT = ($experiments + 'simd') -join ','
    $env:GOTOOLCHAIN = 'local'
    $goRoot = (& $Go env GOROOT).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Could not determine GOROOT.' }
    $invokeArgs = @($GoArgs[0])

    if (-not (Test-Path -LiteralPath (Join-Path $goRoot 'src/simd/maskbits.go'))) {
        $source = Join-Path $PSScriptRoot 'tools/v28-simd'
        $sha = [Security.Cryptography.SHA256]::Create()
        try {
            $hash = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes("$PSScriptRoot`n$goRoot"))).Replace('-', '').Substring(0, 16)
        } finally { $sha.Dispose() }
        $temp = Join-Path ([IO.Path]::GetTempPath()) ('cc-v28-simd-' + $hash)
        [IO.Directory]::CreateDirectory($temp) | Out-Null
        $replace = [ordered]@{}
        $files = [ordered]@{
            'src/simd/v28_maskbits.go' = 'maskbits.go.txt'
            'src/simd/v28_maskbits_stubs.go' = 'maskbits_stubs.go.txt'
            'src/simd/v28_maskbits_emulated.go' = 'maskbits_emulated.go.txt'
            'src/simd/internal/bridge/v28_maskbits_amd64.go' = 'bridge_amd64.go.txt'
            'src/simd/internal/bridge/v28_maskbits_128.go' = 'bridge_128.go.txt'
            'src/simd/archsimd/v28_maskbits_128.go' = 'arch_128.go.txt'
        }
        foreach ($entry in $files.GetEnumerator()) {
            $replace[(Join-Path $goRoot $entry.Key)] = Join-Path $source $entry.Value
        }
        $emulated = Get-Content -LiteralPath (Join-Path $source 'maskbits_emulated.go.txt') -Raw
        $emulated = $emulated.Replace('package simd', 'package bridge').Replace('&& !(', '&& (')
        $bridge = Join-Path $temp 'bridge_emulated.go'
        Write-OverlayFile $bridge $emulated
        $replace[(Join-Path $goRoot 'src/simd/internal/bridge/v28_maskbits_emulated.go')] = $bridge
        $overlay = Join-Path $temp 'overlay.json'
        Write-OverlayFile $overlay (@{ Replace = $replace } | ConvertTo-Json -Depth 3)
        $invokeArgs += "-overlay=$overlay"
        Write-Host "Temporary CL 855881 Mask8s.ToBits overlay: $overlay"
    }
    if ($GoArgs.Count -gt 1) { $invokeArgs += $GoArgs[1..($GoArgs.Count - 1)] }
    & $Go @invokeArgs
    $code = $LASTEXITCODE
} finally {
    $env:GOEXPERIMENT = $oldExperiment
    $env:GOTOOLCHAIN = $oldToolchain
}
exit $code
