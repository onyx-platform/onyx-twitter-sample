#!/bin/sh

/opt/jdk/bin/java -cp /opt/peer.jar twit.core start-peers "$NPEERS" -p :docker
