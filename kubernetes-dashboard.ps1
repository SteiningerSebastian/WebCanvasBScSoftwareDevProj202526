# 1. Fetch the permanent token from the Secret and decode it
Write-Host "Fetching permanent token..." -ForegroundColor Cyan
$base64Token = kubectl get secret admin-user-secret -n kubernetes-dashboard -o jsonpath='{.data.token}'

if (-not $base64Token) {
    Write-Host "Error: Secret 'admin-user-secret' not found. Please create it first." -ForegroundColor Red
    return
}

$token = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($base64Token))

# 2. Copy the decoded token to your clipboard
$token | clip
Write-Host "Permanent token copied to clipboard!" -ForegroundColor Green

# 3. Start the Port Forward in the background
Write-Host "Starting Port-Forward..." -ForegroundColor Cyan
$job = Start-Job -ScriptBlock { 
    kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard-kong-proxy 8443:443 
}

# 4. Open Chrome
Write-Host "Opening Chrome to Dashboard..." -ForegroundColor Yellow
Start-Process chrome.exe "--new-window https://localhost:8443/"

Write-Host "----------------------------------------------------------"
Write-Host "ACTION REQUIRED: Press Ctrl+V in the login box to sign in."
Write-Host "----------------------------------------------------------"

# Keep the window open so the port-forwarding job stays alive
Read-Host "Press [Enter] to stop the port-forward and exit"
Stop-Job $job