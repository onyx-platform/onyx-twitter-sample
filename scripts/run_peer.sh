#!/bin/sh

/usr/bin/java -cp /opt/peer.jar twit.core start-peers "$NPEERS" -p :docker
