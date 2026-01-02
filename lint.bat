@echo off
setlocal EnableExtensions

cls

where go >nul 2>nul
if %errorlevel% neq 0 (
  echo go not found in PATH
  exit /b 1
)

for /f "delims=" %%i in ('go env GOBIN') do set "GOBIN=%%i"
if "%GOBIN%"=="" (
  for /f "delims=" %%i in ('go env GOPATH') do set "GOPATH=%%i"
  set "GOBIN=%GOPATH%\\bin"
)
set "PATH=%GOBIN%;%PATH%"

set "GOLANGCI_LINT=%GOBIN%\\golangci-lint.exe"

::if not exist "%GOLANGCI_LINT%" (
::  echo Installing golangci-lint v2...
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
  if %errorlevel% neq 0 exit /b %errorlevel%
::)

"%GOLANGCI_LINT%" run ./...
set res=%errorlevel%
if %res%==0 (
  echo lint success
) else (
  powershell -command "& {$output = 'lint error'; Write-Host $output -ForegroundColor Red}"
  REM pause
)
exit /b %res%
