# Define the output file path
$outputFile = "DirectoryList.txt"

# Define the specific patterns to exclude
$excludePatterns = @(
    "*\.gradle*", 
    "*build*"
)

Write-Host "Scanning directories..." -ForegroundColor Cyan

# Fetch all files and folders recursively
Get-ChildItem -Path . -Recurse -ErrorAction SilentlyContinue | Where-Object {
    $itemPath = $_.FullName
    $shouldInclude = $true

    # Check against relative patterns
    foreach ($pattern in $excludePatterns) {
        if ($itemPath -like $pattern) {
            $shouldInclude = $false
            break
        }
    }

    return $shouldInclude
} | Select-Object -ExpandProperty FullName | Out-File -FilePath $outputFile

Write-Host "Done! Results saved to $outputFile" -ForegroundColor Green
