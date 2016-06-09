# onyx-twitter-sample
Example of an Onyx application using the live twitter stream

# Usage
1. Set your twitter API keys in docker-compose.yaml
2. `lein do clean, uberjar; docker build -t peerimage .`
3. `docker-compose up`
4. `docker-compose run --entrypoint=java peer "-cp" "/opt/peer.jar" "twit.core" "submit-job" "trending-hashtags" "-p" ":docker"`
