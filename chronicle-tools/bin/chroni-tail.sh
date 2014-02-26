#!/usr/bin/env bash

WHEREAMI=`dirname $0`

# set the classpath
CP=$CP:"$WHEREAMI/../../chronicle/target/classes"
CP=$CP:"$WHEREAMI/../../chronicle-sandbox/target/classes"
CP=$CP:"$WHEREAMI/../../chronicle-slf4j/target/classes"
CP=$CP:"$WHEREAMI/../target/classes"

echo $CP

# run the tool
$JAVA_HOME/bin/java -cp $CP net.openhft.chronicle.tools.slf4j.ChroniTail $@
