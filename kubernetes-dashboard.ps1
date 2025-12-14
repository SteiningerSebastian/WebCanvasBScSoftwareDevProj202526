kubectl create token admin-user --namespace kubernetes-dashboard
Start-Process chrome.exe '--new-window https://localhost:8443/'
kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard-kong-proxy 8443:443