#/bin/sh

# needed to be set as ENVs before:
#env_release_url
#env_tag
#env_credentials
#env_file

filefull=`ls $env_file`
filename=$(basename $filefull)


echo "{\"tag_name\": \"$env_tag\",\"target_commitish\": \"master\",\"name\":\"$env_tag\",\"body\": \"Automatically created by CI\",\"draft\": false,\"prerelease\": false}" > /tmp/release_payload.json

# Create release
curl -u $env_credentials -X POST -H 'Content-Type: application/json' -d @/tmp/release_payload.json $env_release_url > resp.json && cat resp.json

# parse response to create upload_url
upload_url=`cat resp.json | jq '.upload_url' | tr -d '"' | cut -d '{' -f1`
url=$upload_url?name=$filename

if [ "$upload_url" == "null" ]
then
    echo "Failed to create release, aborting."
    exit 1
fi

curl -u $env_credentials -H "Accept: application/vnd.github.manifold-preview" -H "Content-Type: application/zip" --data-binary @$filefull "$url" > resp.json && cat resp.json
