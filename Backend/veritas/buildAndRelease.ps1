# From repo root
$shortSha = (git rev-parse --short HEAD)
$version = "alpha"
$imageName = "sebastiansteininger/veritas"
$tag1 = "${imageName}:${version}"
$tag2 = "${imageName}:alpha-$shortSha"
$dockerfile = "./veritas/Dockerfile"

# Build with labels
docker build --file $dockerfile `
  --label "org.opencontainers.image.version=$version" `
  --label "org.opencontainers.image.revision=$shortSha" `
  --label "org.opencontainers.image.created=$(Get-Date -Format o)" `
  -t $tag1 -t $tag2 .

# Push both tags
docker push $tag1
docker push $tag2

kubectl set image statefulset/veritas veritas=sebastiansteininger/veritas:alpha -n default
kubectl rollout restart statefulset/veritas -n default