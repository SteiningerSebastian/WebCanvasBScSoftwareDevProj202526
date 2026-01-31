# 1. Fetch the token (No decoding needed for 'kubectl create token')
# Note: Token lifetime is limited by the cluster's API server settings.
# You can try increasing $tokenDuration, but the cluster may cap it.
$tokenDuration = "24h"
Write-Host "Fetching token..." -ForegroundColor Cyan
$token = kubectl create token headlamp-admin -n kube-system --duration=$tokenDuration

if (-not $token) {
    Write-Host "Error: Could not generate token. Check if ServiceAccount 'headlamp-admin' exists." -ForegroundColor Red
    return
}

# 2. Copy the raw token to your clipboard
$token.Trim() | clip
Write-Host "Token copied to clipboard!" -ForegroundColor Green

# 3. Start the Port Forward in the background
Write-Host "Starting Port-Forward..." -ForegroundColor Cyan
$job = Start-Job -ScriptBlock { 
    # Using 8443:80 as per your original script
    kubectl port-forward -n kube-system service/my-headlamp 8443:80
}

# 4. Open Chrome
Write-Host "Opening Chrome to Dashboard..." -ForegroundColor Yellow
# Note: Using --ignore-certificate-errors can be helpful for local dev HTTPS
Start-Process chrome.exe "--new-window http://127.0.0.1:8443/c/main/token"

Write-Host "----------------------------------------------------------"
Write-Host "ACTION REQUIRED: Press Ctrl+V in the login box to sign in."
Write-Host "----------------------------------------------------------"

# Keep the window open so the port-forwarding job stays alive
Read-Host "Press [Enter] to stop the port-forward and exit"
Stop-Job $job
Remove-Job $job