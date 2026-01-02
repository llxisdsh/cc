@echo off
setlocal EnableExtensions

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

::where gofumpt >nul 2>nul
::if %errorlevel% neq 0 (
::  echo Installing gofumpt...
  go install mvdan.cc/gofumpt@latest
  if %errorlevel% neq 0 exit /b %errorlevel%
::)

gofumpt -l -w .
if %errorlevel% neq 0 exit /b %errorlevel%

gofmt -s -w .
exit /b %errorlevel%
