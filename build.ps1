$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    $env:CGO_ENABLED = '1'
    $target = & go env GOOS
    if ($LASTEXITCODE -ne 0) { throw 'Failed to determine Go target' }
    if ($target -notin @('linux', 'darwin')) {
        throw 'Only Linux and macOS builds are supported (Unix credential ownership checks).'
    }
    $commit = $env:CI_COMMIT_SHA
    if (-not $commit) {
        $commit = & git rev-parse HEAD 2>$null
        if ($LASTEXITCODE -ne 0) { $commit = 'unknown' }
    }
    $tag = $env:CI_COMMIT_TAG
    if (-not $tag) { $tag = 'unknown' }
    $buildTime = $env:CI_BUILD_TIME
    if (-not $buildTime) { $buildTime = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ') }
    & go build -mod=readonly -trimpath -tags goolm -ldflags "-X main.tag=$tag -X main.commit=$commit -X main.buildTime=$buildTime" -o chatgpt-dots ./cmd/chatgpt-dots
    if ($LASTEXITCODE -ne 0) { throw 'ChatGPT Dots build failed' }
} finally {
    Pop-Location
}
