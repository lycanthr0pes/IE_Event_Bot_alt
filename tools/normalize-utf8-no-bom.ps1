param(
    [switch]$Apply,
    [switch]$IncludeUntracked,
    [switch]$ForceCp932Fallback
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-GitFiles {
    param([bool]$WithUntracked)
    if ($WithUntracked) {
        $lines = git ls-files --cached --others --exclude-standard
    } else {
        $lines = git ls-files
    }
    @($lines | Where-Object { $_ -and (Test-Path $_ -PathType Leaf) })
}

function Test-IsLikelyBinary {
    param(
        [string]$Path,
        [byte[]]$Bytes
    )

    $binaryExt = @(
        ".png",".jpg",".jpeg",".gif",".webp",".avif",".ico",".bmp",".tif",".tiff",
        ".pdf",".zip",".gz",".bz2",".xz",".7z",".tar",".rar",
        ".woff",".woff2",".ttf",".otf",".eot",
        ".mp3",".wav",".ogg",".m4a",".mp4",".mov",".avi",".webm",
        ".exe",".dll",".so",".dylib",".class",".pyc",".pyd",".db",".sqlite",".bin"
    )

    $ext = [System.IO.Path]::GetExtension($Path).ToLowerInvariant()
    if ($binaryExt -contains $ext) { return $true }
    if ($Bytes.Length -eq 0) { return $false }
    # If file contains NUL bytes, treat it as binary.
    foreach ($b in $Bytes) {
        if ($b -eq 0) { return $true }
    }
    $false
}

function Try-DecodeUtf8Strict {
    param([byte[]]$Bytes)
    try {
        $enc = [System.Text.UTF8Encoding]::new($false, $true)
        @{ Ok = $true; Text = $enc.GetString($Bytes) }
    } catch {
        @{ Ok = $false; Text = $null }
    }
}

function Convert-FileToUtf8NoBom {
    param(
        [string]$Path,
        [switch]$DoApply,
        [switch]$AllowCp932Fallback
    )

    $bytes = [System.IO.File]::ReadAllBytes($Path)
    if (Test-IsLikelyBinary -Path $Path -Bytes $bytes) {
        return @{ Status = "skip-binary"; Changed = $false; Reason = "" }
    }

    $hasUtf8Bom = $bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF
    $sourceBytes = if ($hasUtf8Bom) { $bytes[3..($bytes.Length - 1)] } else { $bytes }

    $decoded = Try-DecodeUtf8Strict -Bytes $sourceBytes
    $sourceEncoding = "utf8"

    if (-not $decoded.Ok) {
        if (-not $AllowCp932Fallback) {
            return @{ Status = "skip-nonutf8"; Changed = $false; Reason = "utf8-decode-failed" }
        }
        try {
            $cp932 = [System.Text.Encoding]::GetEncoding(932)
            $decoded = @{ Ok = $true; Text = $cp932.GetString($bytes) }
            $sourceEncoding = "cp932"
        } catch {
            return @{ Status = "skip-nonutf8"; Changed = $false; Reason = "cp932-decode-failed" }
        }
    }

    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    $newBytes = $utf8NoBom.GetBytes([string]$decoded.Text)

    $same = $bytes.Length -eq $newBytes.Length
    if ($same) {
        for ($i = 0; $i -lt $bytes.Length; $i++) {
            if ($bytes[$i] -ne $newBytes[$i]) {
                $same = $false
                break
            }
        }
    }
    if ($same) {
        return @{ Status = "unchanged"; Changed = $false; Reason = "" }
    }

    if ($DoApply) {
        [System.IO.File]::WriteAllBytes($Path, $newBytes)
    }

    $reason = if ($hasUtf8Bom) {
        "remove-bom"
    } elseif ($sourceEncoding -ne "utf8") {
        "transcode-cp932-to-utf8"
    } else {
        "normalize-utf8"
    }
    @{ Status = "changed"; Changed = $true; Reason = $reason }
}

$files = Get-GitFiles -WithUntracked:$IncludeUntracked
if (-not $files -or $files.Count -eq 0) {
    Write-Output "No files found."
    exit 0
}

$changed = New-Object System.Collections.Generic.List[string]
$skipped = New-Object System.Collections.Generic.List[string]
$errors = New-Object System.Collections.Generic.List[string]

foreach ($f in $files) {
    try {
        $result = Convert-FileToUtf8NoBom -Path $f -DoApply:$Apply -AllowCp932Fallback:$ForceCp932Fallback
        if ($result.Status -eq "changed") {
            $changed.Add("$f [$($result.Reason)]")
        } elseif ($result.Status -like "skip-*") {
            $skipped.Add("$f [$($result.Reason)]")
        }
    } catch {
        $errors.Add("$f [$($_.Exception.Message)]")
    }
}

Write-Output "Mode: $(if ($Apply) { 'apply' } else { 'dry-run' })"
Write-Output "Scanned: $($files.Count)"
Write-Output "Changed: $($changed.Count)"
Write-Output "Skipped: $($skipped.Count)"
Write-Output "Errors:  $($errors.Count)"

if ($changed.Count -gt 0) {
    Write-Output ""
    Write-Output "Changed files:"
    $changed | ForEach-Object { Write-Output "  $_" }
}

if ($errors.Count -gt 0) {
    Write-Output ""
    Write-Output "Errors:"
    $errors | ForEach-Object { Write-Output "  $_" }
    exit 1
}
