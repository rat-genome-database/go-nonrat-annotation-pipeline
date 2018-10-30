# load mouse and human GO Annotations into FULL_ANNOT table
#
. /etc/profile
APPNAME=mouseandhumanGOAnnotationPipeline
HOMEDIR=/home/rgddata/pipelines/$APPNAME
cd $HOMEDIR

java -Dspring.config=$HOMEDIR/../properties/default_db.xml \
  -Dlog4j.configuration=file://$HOMEDIR/properties/log4j.properties \
  -jar ./$APPNAME.jar "$@"

echo "Unprocessed Source Databases (count db):"
TODAY=`date +%Y%m%d`
zcat $HOMEDIR/data/${TODAY}_goa_human*.gaf.gz \
  | grep -v '^!' \
  | grep -v '^UniProtKB' | grep -v '^HGNC' | grep -v '^Ensembl' \
  | awk '{print $1}' \
  | sort | uniq -c
