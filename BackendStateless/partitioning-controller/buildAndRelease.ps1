# From repo root
$shortSha = (git rev-parse --short HEAD)
$version = "dev"
$imageName = "sebastiansteininger/noredb-partitioning-controller"
$tag1 = "${imageName}:${version}"
$tag2 = "${imageName}:dev-$shortSha"
$dockerfile = "./partitioning-controller/Dockerfile"

# Pull previous image for cache (ignore errors if it doesn't exist)
docker pull $tag1 2>$null

# Build with cache (context is parent directory to include workspace modules)
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
kubectl set image deployment/partitioning-controller partitioning-controller=sebastiansteininger/noredb-partitioning-controller:dev -n default
kubectl rollout restart deployment/partitioning-controller -n default

# Wait for rollout to complete
kubectl rollout status deployment/partitioning-controller -n default