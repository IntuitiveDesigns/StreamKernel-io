# Copyright (c) 2026 Steven Lopez
# SPDX-License-Identifier: LicenseRef-SSAL-1.0
#
# Licensed under the StreamKernel Source Available License (SSAL) v1.0.
# See the LICENSE file in the project root for the full license text.

$ErrorActionPreference = "Stop"

$root = Get-Location
$encoding = [System.Text.UTF8Encoding]::new($false)

$apacheModulePrefixes = @(
    "streamkernel-api\",
    "streamkernel-spi\",
    "streamkernel-metrics\metrics-api\"
)

function Get-RepoRelativePath {
    param([string]$FullName)

    return $FullName.Substring($root.Path.Length + 1)
}

function Test-IsApacheSdkFile {
    param([string]$RelativePath)

    foreach ($prefix in $apacheModulePrefixes) {
        if ($RelativePath.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
    }
    return $false
}

function Get-ExpectedSpdx {
    param([string]$RelativePath)

    if (Test-IsApacheSdkFile -RelativePath $RelativePath) {
        return "Apache-2.0"
    }
    return "LicenseRef-SSAL-1.0"
}

function Get-TrackedJavaFiles {
    return Get-ChildItem -Path $root -Recurse -Filter "*.java" -File | Where-Object {
        $_.FullName -notmatch "\\(build|bin|\.git|\.gradle|\.gradle-user|node_modules|dist|logs)\\"
    }
}

Write-Host "=== StreamKernel Release Prep Tool ===" -ForegroundColor Cyan

# ==========================================
# 1. Dependency Auditor (Jackson Version Check)
# ==========================================
Write-Host "`n[1/4] Auditing build.gradle files for hardcoded Jackson versions..." -ForegroundColor Yellow

$gradleFiles = Get-ChildItem -Path $root -Recurse -Filter "build.gradle" -File | Where-Object {
    $_.FullName -notmatch "\\(build|bin|\.git|\.gradle|\.gradle-user|node_modules|dist|logs)\\"
}
$versionReport = @()

foreach ($file in $gradleFiles) {
    $content = Get-Content $file.FullName
    $jacksonLines = $content | Select-String -Pattern 'jackson.*?[''"](\d+\.\d+\.\d+)[''"]'
    foreach ($match in $jacksonLines) {
        $versionReport += [PSCustomObject]@{
            Module  = Get-RepoRelativePath -FullName $file.FullName
            Library = $match.Line.Trim()
        }
    }
}

if ($versionReport.Count -gt 0) {
    $versionReport | Sort-Object Module | Format-Table -AutoSize
    Write-Host "[ACTION REQUIRED] Found hardcoded versions above. Move these to root build.gradle." -ForegroundColor Red
} else {
    Write-Host "[OK] No hardcoded Jackson versions found." -ForegroundColor Green
}

# ==========================================
# 2. Mixed-License Header Audit
# ==========================================
Write-Host "`n[2/4] Auditing Java headers for Apache SDK vs SSAL runtime..." -ForegroundColor Yellow

$missingHeaders = @()
$mismatchedHeaders = @()

foreach ($file in Get-TrackedJavaFiles) {
    $relativePath = Get-RepoRelativePath -FullName $file.FullName
    $expectedSpdx = Get-ExpectedSpdx -RelativePath $relativePath
    $content = [System.IO.File]::ReadAllText($file.FullName, [System.Text.Encoding]::UTF8)

    $match = [regex]::Match($content, 'SPDX-License-Identifier:\s*(?<id>[A-Za-z0-9\.\-:]+)')
    if (-not $match.Success) {
        $missingHeaders += $relativePath
        continue
    }

    $actualSpdx = $match.Groups["id"].Value.Trim()
    if ($actualSpdx -ne $expectedSpdx) {
        $mismatchedHeaders += [PSCustomObject]@{
            File     = $relativePath
            Expected = $expectedSpdx
            Actual   = $actualSpdx
        }
    }
}

if ($missingHeaders.Count -gt 0) {
    Write-Host "[WARNING] Java files missing SPDX headers:" -ForegroundColor Yellow
    $missingHeaders | Sort-Object | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
} else {
    Write-Host "[OK] No Java files are missing SPDX headers." -ForegroundColor Green
}

if ($mismatchedHeaders.Count -gt 0) {
    Write-Host "[WARNING] Java files with the wrong license header:" -ForegroundColor Yellow
    $mismatchedHeaders | Sort-Object File | ForEach-Object {
        Write-Host ("  - {0} (expected {1}, found {2})" -f $_.File, $_.Expected, $_.Actual) -ForegroundColor Yellow
    }
} else {
    Write-Host "[OK] Java headers match the mixed-license boundary." -ForegroundColor Green
}

# ==========================================
# 3. Repo Notice / License File Audit
# ==========================================
Write-Host "`n[3/4] Auditing required license files..." -ForegroundColor Yellow

$requiredFiles = @(
    "LICENSE",
    "LICENSE-APACHE-2.0.txt",
    "LICENSE-HISTORY.md",
    "TRADEMARK-POLICY.md",
    "NOTICE",
    "streamkernel-api\NOTICE",
    "streamkernel-spi\NOTICE",
    "streamkernel-metrics\metrics-api\NOTICE"
)

$missingRequired = @()
foreach ($path in $requiredFiles) {
    if (-not (Test-Path -LiteralPath (Join-Path $root $path))) {
        $missingRequired += $path
    }
}

if ($missingRequired.Count -gt 0) {
    Write-Host "[WARNING] Missing required license files:" -ForegroundColor Yellow
    $missingRequired | ForEach-Object { Write-Host "  - $_" -ForegroundColor Yellow }
} else {
    Write-Host "[OK] Required root and SDK notice files are present." -ForegroundColor Green
}

# ==========================================
# 4. Final Build Verification
# ==========================================
Write-Host "`n[4/4] Verifying build integrity (skipping tests)..." -ForegroundColor Yellow
cmd /c "gradlew.bat clean build -x test"

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[SUCCESS] READY TO REVIEW." -ForegroundColor Green
    Write-Host "Suggested commands:" -ForegroundColor Gray
    Write-Host "  git add ." -ForegroundColor White
    Write-Host "  git commit -m 'chore: apply mixed Apache SDK and SSAL runtime licensing'" -ForegroundColor White
} else {
    Write-Host "`n[FAIL] Build failed. Fix errors before committing." -ForegroundColor Red
}
