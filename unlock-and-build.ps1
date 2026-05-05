$ErrorActionPreference = "Stop"

Get-Process java,gradle* -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 2

if (Test-Path .\streamkernel-app\build) {
    Remove-Item .\streamkernel-app\build -Recurse -Force
}

gradle build -x test --no-daemon