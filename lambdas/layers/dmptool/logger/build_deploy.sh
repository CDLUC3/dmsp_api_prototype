# Set your layer name here
ZIP_NAME_SUFFIX=logger-layer
LAYER_NAME=Logger

if [ $# -lt 1 ]; then
  echo 'You specify the environment! (e.g. dev, stg, prd)'
  exit 1
fi

# Run npm install and compile/build
echo "Compiling ..."
npm install
npm run build

# Remove all of the Typescript stuff used to compile.
echo "Removing typescript from node_modules in the dist dir ..."
rm -rf dist/node_modules/.bin/tsserver
rm -rf dist/node_modules/.bin/tsc
rm -rf dist/node_modules/typescript

# Generate the ZIP artifact
echo "creating ZIP artifact ..."
zip -r "dmptool-${ZIP_NAME_SUFFIX}-${1}.zip" dist

# Publish the layer
echo "Publishing ZIP archive to S3 ..."
VERSION_ARN=$(aws lambda publish-layer-version --layer-name "dmptool-${ZIP_NAME_SUFFIX}-${1}" \
    --description "${LAYER_NAME} helper functions for NodeJS Lambda functions" \
    --zip-file "fileb://dmptool-${ZIP_NAME_SUFFIX}-${1}.zip" \
    --compatible-runtimes nodejs20.x \
    --compatible-architectures "arm64" | jq -r '.LayerVersionArn')

# Publish the new Layer ARN to SSM. The Lambda Functions fetch the value when they are built
echo "Layer published: ${VERSION_ARN}. Updating current version in SSM ..."
aws ssm put-parameter --name "/uc3/dmp/hub/${1}/DMPTool${LAYER_NAME}LayerArn" \
    --value $VERSION_ARN \
    --type "String" \
    --overwrite

echo "DO NOT FORGET! Update all Lambda functions that reference this layer!"
