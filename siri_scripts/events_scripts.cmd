@echo off
:: Close all active Command Prompt windows
taskkill /F /FI "WINDOWTITLE eq Trip-Performance"
taskkill /F /FI "WINDOWTITLE eq Trip-Deviation"
taskkill /F /FI "WINDOWTITLE eq Low-speed"
taskkill /F /FI "WINDOWTITLE eq Bunching"


@echo off

echo Running Trip-Performance Script.
start "Trip-Performance" cmd /c "C:\DEV\updated_scripts\trip_performance.cmd"

:: Wait for 10 minutes i.e., 600 secs before Trip-Deviation script so that required tables are created.
timeout /t 600 /nobreak >nul

echo Running Trip-Deviation Script.
start "Trip-Deviation" cmd /c "C:\DEV\updated_scripts\trip_deviation.cmd"

:: Wait for 20 seconds before running Low-speed Script.
timeout /t 20 /nobreak >nul

echo Running Low-speed Script.
start "Low-speed" cmd /c "C:\DEV\updated_scripts\trip_low_speed.cmd"


:: Wait for 300 seconds before Bunching script.
timeout /t 300 /nobreak >nul

echo Running Bunching Script.
start "Bunching" cmd /c "C:\DEV\updated_scripts\trip_bunching.cmd"
