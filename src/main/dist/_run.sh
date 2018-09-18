#!/usr/bin/env bash
# shell script to run mouseandhumanGOAnnotation pipeline
. /etc/profile

APPNAME=mouseandhumanGOAnnotationPipeline
APPDIR=/home/rgddata/pipelines/$APPNAME

cd $APPDIR
pwd
DB_OPTS="-Dspring.config=$APPDIR/../properties/default_db.xml"
LOG4J_OPTS="-Dlog4j.configuration=file://$APPDIR/properties/log4j.properties"
export MOUSEANDHUMAN_GO_ANNOTATION_PIPELINE_OPTS="$DB_OPTS $LOG4J_OPTS"

bin/$APPNAME "$@" 2>&1
