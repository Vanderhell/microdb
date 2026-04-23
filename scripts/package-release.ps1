# SPDX-License-Identifier: MIT
param(
    [string]$Version = "dev",
    [string]$BuildDir = "build_verify",
    [string]$Config = "Release",
    [string]$Platform = "windows-x64"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$stageRoot = Join-Path $root "dist"
$packageName = "loxdb-$Version-$Platform"
$installDir = Join-Path $stageRoot $packageName
$archivePath = Join-Path $stageRoot "$packageName.zip"
$checksumPath = "$archivePath.sha256"

if (Test-Path $installDir) {
    Remove-Item -Recurse -Force $installDir
}
if (Test-Path $archivePath) {
    Remove-Item -Force $archivePath
}
if (Test-Path $checksumPath) {
    Remove-Item -Force $checksumPath
}

New-Item -ItemType Directory -Force -Path $stageRoot | Out-Null

cmake --install $BuildDir --config $Config --prefix $installDir
Compress-Archive -Path $installDir -DestinationPath $archivePath

$hash = (Get-FileHash $archivePath -Algorithm SHA256).Hash.ToLower()
"$hash  $(Split-Path -Leaf $archivePath)" | Out-File -FilePath $checksumPath -Encoding ascii

Write-Output $archivePath
Write-Output $checksumPath
