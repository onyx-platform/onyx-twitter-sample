# onyx-twitter-sample
Example of an Onyx application using the live twitter stream

# Usage
- `lein do clean, uberjar; docker build -t peerimage .`
- `docker-compose up`
- `docker-compose run --entrypoint=java peer "-cp" "/opt/peer.jar" "twit.core" "submit-job" "trending-hashtags" "-p" ":docker"`
