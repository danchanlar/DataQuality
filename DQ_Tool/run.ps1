# Run DQ_Tool Streamlit app
# Usage: .\run.ps1 [port]

param([int]$port = 8520)

$app = "dq_tool/ui/app.py"

Write-Host "Starting DQ_Tool on http://localhost:$port..." -ForegroundColor Green
.\.venv\Scripts\python -m streamlit run $app --server.port $port
