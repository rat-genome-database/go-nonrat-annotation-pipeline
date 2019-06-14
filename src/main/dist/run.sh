# load mouse and human GO Annotations into FULL_ANNOT table
#
. /etc/profile
APPNAME=mouseandhumanGOAnnotationPipeline
HOMEDIR=/home/rgddata/pipelines/$APPNAME

SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
if [ "$SERVER" = "REED" ]; then
  EMAIL_LIST=rgd.developers@mcw.edu,jrsmith@mcw.edu
fi

cd $HOMEDIR

java -Dspring.config=$HOMEDIR/../properties/default_db2.xml \
  -Dlog4j.configuration=file://$HOMEDIR/properties/log4j.properties \
  -jar lib/$APPNAME.jar "$@" > run.log 2>&1


echo "" >> $HOMEDIR/log/status_summary.log
echo "Unprocessed Source Databases (count db):" >> $HOMEDIR/log/status_summary.log

TODAY=`date +%Y%m%d`
zcat $HOMEDIR/data/${TODAY}_goa_human*.gaf.gz \
  | grep -v '^!' \
  | grep -v '^UniProtKB' | grep -v '^HGNC' | grep -v '^Ensembl' \
  | awk '{print $1}' \
  | sort | uniq -c       >> $HOMEDIR/log/status_summary.log

mailx -s "[$SERVER] Mouse and Human GO Annotation Pipeline" $EMAIL_LIST < $HOMEDIR/log/status_summary.log
