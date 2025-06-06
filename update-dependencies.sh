#!/bin/bash

if [ $# -lt 2 ]; then
  echo 'You MUST specify the environment and whether or not to increment the Ruby gem versions!'
  echo '    Usage: ./update-dependencies.sh dev true'
  exit 1
fi

echo "Upgrading all dependencies for environment: $1"
echo "==============================================="
echo ""

# Set the log level based on the environment
if [ "$1" == "prd" ]; then
  export LOG_LEVEL="error"
else
  export LOG_LEVEL="debug"
fi

# If the second argument is "true", increment Ruby gem versions and publish them
if [ "$2" == "true" ]; then
  # Base path to the gems
  BASE_DIR="./gems"
  # Array of gem directory names (edit as needed)
  GEMS=("uc3-dmp-api-core" "uc3-dmp-citation" "uc3-dmp-cloudwatch" "uc3-dmp-cognito" "uc3-dmp-dynamo" "uc3-dmp-event-bridge" "uc3-dmp-external-api" "uc3-dmp-id" "uc3-dmp-provenance" "uc3-dmp-rds" "uc3-dmp-s3")

  # Loop through each gem
  for GEM in "${GEMS[@]}"; do
  echo "Upgrading gem $GEM ..."
  echo "---------------------------------------------------"
  GEM_DIR="$BASE_DIR/$GEM"
  VERSION_FILE="./lib/$GEM/version.rb"
  cd "$GEM_DIR" || exit 1
  (
    if [ ! -f "$VERSION_FILE" ]; then
      echo "  Version file not found: $VERSION_FILE"
      exit 1
    fi

    # Clean up old .gem files
    rm -f *.gem

    # Install and update dependencies
    bundle install
    bundle update

    # Increment patch version
    PATCH_VERSION=$(grep -Eo "VERSION = '[0-9]+\.[0-9]+\.[0-9]+'" "$VERSION_FILE" | sed -E "s/.*'[^']*\.([^']*)'/\1/")
    NEW_PATCH_VERSION=$(($PATCH_VERSION + 1))
    echo "** Current patch version: $PATCH_VERSION -> New patch version: $NEW_PATCH_VERSION"

    sed -i '' -E "s/(VERSION = '([0-9]+)\.([0-9]+)\.)[0-9]+'/\1${NEW_PATCH_VERSION}'/" "$VERSION_FILE"

    # Build and publish gem
    gem build "$GEM.gemspec"
    gem push *.gem
  )
  cd ../../

  echo "---------------------------------------------------"
  echo "Gem $GEM upgraded and published."
  echo ""
  done

  echo "Sleeping for 30 seconds to allow gem updates to propagate ..."
  sleep 30
else
  echo "Skipping Ruby gem version increment and publication."
  echo "---------------------------------------------------"
  echo ""
fi

# Upgrade the landing page JS dependencies
echo "Upgrading landing page dependencies ..."
echo "---------------------------------------------------"
(
    cd ./landing_page || exit 1
    npm install 
    npm upgrade 
    bundle install
    bundle update
    ruby build_deploy.rb $1 
    cd ..
)
echo "---------------------------------------------------"
echo "Landing page dependencies upgraded."
echo ""

# Upgrade the baseline layer dependencies. This autodeploys the harvester, indexer and util lambdas (Ruby)
echo "Upgrading baseline Lambda Layer dependencies and dependent Lambdas ..."
echo "---------------------------------------------------"
(
    cd ./lambdas/layers/baseline || exit 1 
    bundle install 
    bundle update 
    ruby sam_build_deploy.rb $1 true true $LOG_LEVEL
    cd ../../..
)
echo "---------------------------------------------------"
echo "Baseline layer dependencies upgraded."
echo ""

# Upgrade the dmptool layer dependencies (JS)
echo "Upgrading DMPTool Lambda Layer dependencies ..."
echo "---------------------------------------------------"
(
    cd ./lambdas/layers/dmptool || exit 1
    npm install 
    npm upgrade 
    ./build_deploy.sh $1
    cd ../../..
)
echo "---------------------------------------------------"
echo "DMPTool layer dependencies upgraded."
echo ""

# Build the JS Lambdas so their dist directories are available
LAMBDAS=("api/get_dmps_downloads" "api/put_dmps_uploads", "integrations/dmp_extractor")
for LAMBDA in "${LAMBDAS[@]}"; do
    echo "Building lambdas/$LAMBDA ..."
    echo "---------------------------------------------------"
    (
        cd ./lambdas/$LAMBDA || exit 1
        npm install 
        npm upgrade 
        npm run build
        cd ..
    )
    echo "---------------------------------------------------"
    echo "Done building lambdas/$LAMBDA"
done

# Upgrade the api layer dependencies. This autodeploys the API lambdas (Ruby)
echo "Upgrading API Lambda Layer dependencies and dependent Lambdas ..."
echo "---------------------------------------------------"
(
    cd ./lambdas/layers/api || exit 1
    bundle install 
    bundle update 
    ruby sam_build_deploy.rb $1 true true $LOG_LEVEL
    cd ../../..
)
echo "---------------------------------------------------"
echo "API layer dependencies upgraded."
echo ""

# Upgrade the COKI integration layer (JS)
echo "Upgrading COKI integration layer dependencies ..."
echo "---------------------------------------------------"
(
    cd ./lambdas/integrations/dmp_extractor || exit 1
    npm install 
    npm upgrade 
    ruby sam_build_deploy.rb $1 true true $LOG_LEVEL
    cd ../../..
)
echo "---------------------------------------------------"
echo "COKI integration layer dependencies upgraded."
echo ""

# If the env is not prod update the swagger site
if [ "$1" == "prd" ]; then
  echo "Skipping Swagger site upgrade in production environment."
  echo "---------------------------------------------------"
else
  # Upgrade amd redeploy the swagger site
  echo "Upgrading Swagger site dependencies ..."
  echo "---------------------------------------------------"
  CURRENT_SWAGGER_VER=$(curl -s https://api.github.com/repos/swagger-api/swagger-ui/releases/latest | grep '"tag_name"' | cut -d '"' -f 4)
  (
    cd ./swagger || exit 1
    npm install 
    npm upgrade 
    bundle install
    bundle update
    # Pass in the current Swagger version without the 'v' prefix
    ruby build_openapi_spec.rb $1 ${CURRENT_SWAGGER_VER#v}
    cd ..
  )
  echo "---------------------------------------------------"
  echo "Swagger site dependencies upgraded."
  echo ""
fi

echo "==============================================="
echo "All dependencies upgraded for environment: $1"
