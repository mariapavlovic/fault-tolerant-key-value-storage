#!/bin/bash

source settings.sh              # ZKSTRING=manta.uwaterloo.ca:2181, JAVA_HOME

JAVA_CC=$JAVA_HOME/bin/javac
export CLASSPATH=".:lib/*"


echo --- Creating ZooKeeper node
./build.sh
$JAVA_HOME/bin/java CreateZNode $ZKSTRING /$USER
