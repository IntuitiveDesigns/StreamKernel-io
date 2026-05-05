# Get CPU Details
$cpu = Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors, MaxClockSpeed
$cpuInfo = "$($cpu.Name) ($($cpu.NumberOfCores) Cores / $($cpu.NumberOfLogicalProcessors) Threads) @ $($cpu.MaxClockSpeed)MHz"

# Get RAM Details
$ramModules = Get-CimInstance Win32_PhysicalMemory
$totalRAM = ($ramModules | Measure-Object -Property Capacity -Sum).Sum / 1GB
$ramSpeed = $ramModules[0].Speed
$ramInfo = "{0:N2} GB @ {1}MHz" -f $totalRAM, $ramSpeed

# Get GPU Details
$gpus = Get-CimInstance Win32_VideoController | Select-Object Name, DriverVersion, VideoProcessor
$gpuInfo = $gpus | ForEach-Object { "$($_.Name) (Driver: $($_.DriverVersion))" }

# Showcase Output
Write-Host "--- BENCHMARK SYSTEM SPECS ---" -ForegroundColor Cyan
Write-Host "CPU: " -NoNewline; Write-Host $cpuInfo -ForegroundColor Green
Write-Host "RAM: " -NoNewline; Write-Host $ramInfo -ForegroundColor Green
Write-Host "GPU: " -NoNewline; Write-Host ($gpuInfo -join " | ") -ForegroundColor Green
Write-Host "------------------------------"