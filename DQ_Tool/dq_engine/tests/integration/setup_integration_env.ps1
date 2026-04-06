param(
    [string]$Server = "localhost",
    [int]$Port = 1433,
    [string]$Database = "CSBDATA_DEV",
    [string]$User = "sa",
    [string]$Password = "YourPassword",
    [switch]$UseWindowsAuth,
    [switch]$RunTests
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
$dqFolder = Join-Path $repoRoot "DQ"
$seedScript = Join-Path $PSScriptRoot "seed_integration_data.sql"
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"

function Invoke-SqlFile {
    param(
        [Parameter(Mandatory=$true)][string]$FilePath,
        [Parameter(Mandatory=$true)][string]$ServerName,
        [Parameter(Mandatory=$true)][string]$DatabaseName,
        [string]$Username,
        [string]$Pwd,
        [switch]$IntegratedAuth
    )

    if (-not (Test-Path $FilePath)) {
        throw "SQL file not found: $FilePath"
    }

    Write-Host "Running: $FilePath" -ForegroundColor Cyan
    if ($IntegratedAuth) {
        sqlcmd -S $ServerName -d $DatabaseName -E -b -i $FilePath
    }
    else {
        sqlcmd -S $ServerName -d $DatabaseName -U $Username -P $Pwd -b -i $FilePath
    }

    if ($LASTEXITCODE -ne 0) {
        throw "sqlcmd failed while running: $FilePath (exit code: $LASTEXITCODE)"
    }
}

$serverWithPort = "$Server,$Port"

Write-Host "Validating SQL connectivity to $serverWithPort ..." -ForegroundColor Yellow
if ($UseWindowsAuth) {
    sqlcmd -S $serverWithPort -d $Database -E -Q "SELECT @@SERVERNAME AS ServerName, DB_NAME() AS CurrentDb;" -b | Out-Null
}
else {
    sqlcmd -S $serverWithPort -d $Database -U $User -P $Password -Q "SELECT @@SERVERNAME AS ServerName, DB_NAME() AS CurrentDb;" -b | Out-Null
}

if ($LASTEXITCODE -ne 0) {
    throw "SQL connectivity validation failed for $serverWithPort (exit code: $LASTEXITCODE)"
}

Write-Host "SQL connectivity OK" -ForegroundColor Green

$schemaFiles = @(
    "00_Create_Schema.sql",
    "01_Create_DQ_Rules.sql",
    "02_Create_DQ_RuleExecutions.sql",
    "03_Create_DQ_RuleViolations.sql",
    "04_Create_DQ_ExecutionSessions.sql",
    "05_Indexes_And_FKs.sql"
)

foreach ($f in $schemaFiles) {
    $full = Join-Path $dqFolder $f
    Invoke-SqlFile -FilePath $full -ServerName $serverWithPort -DatabaseName $Database -Username $User -Pwd $Password -IntegratedAuth:$UseWindowsAuth
}

Invoke-SqlFile -FilePath $seedScript -ServerName $serverWithPort -DatabaseName $Database -Username $User -Pwd $Password -IntegratedAuth:$UseWindowsAuth

Write-Host "Integration DB setup completed successfully." -ForegroundColor Green

if ($RunTests) {
    if (-not (Test-Path $pythonExe)) {
        throw "Python executable not found at: $pythonExe"
    }

    if ($UseWindowsAuth) {
        $connString = "Driver={ODBC Driver 18 for SQL Server};Server=$serverWithPort;Database=$Database;Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;"
    }
    else {
        $connString = "Driver={ODBC Driver 18 for SQL Server};Server=$serverWithPort;Database=$Database;Uid=$User;Pwd=$Password;Encrypt=no;TrustServerCertificate=yes;"
    }

    Write-Host "Running integration tests..." -ForegroundColor Yellow
    & $pythonExe -m pytest -m integration -vv -s --conn-string $connString --run-integration
}
