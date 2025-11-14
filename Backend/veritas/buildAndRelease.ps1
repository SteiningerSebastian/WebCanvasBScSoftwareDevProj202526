# From repo root
$shortSha = (git rev-parse --short HEAD)
$version = "alpha"
$imageName = "sebastiansteininger/veritas"
$tag1 = "${imageName}:${version}"
$tag2 = "${imageName}:alpha-$shortSha"
$dockerfile = "./veritas/Dockerfile"

# Pull previous image for cache (ignore errors if it doesn't exist)
docker pull $tag1 2>$null

# Build with cache
$env:DOCKER_BUILDKIT=1
docker build --file $dockerfile `
  --cache-from $tag1 `
  --label "org.opencontainers.image.version=$version" `
  --label "org.opencontainers.image.revision=$shortSha" `
  --label "org.opencontainers.image.created=$(Get-Date -Format o)" `
  -t $tag1 -t $tag2 .

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed"
    exit 1
}

# Push both tags
docker push $tag1
docker push $tag2

# Deploy to Kubernetes
kubectl set image statefulset/veritas veritas=sebastiansteininger/veritas:alpha -n default
kubectl rollout restart statefulset/veritas -n default

# Wait for rollout to complete
kubectl rollout status statefulset/veritas -n default