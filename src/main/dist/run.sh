# load GO Annotations for mouse/human/dog/pig into FULL_ANNOT table
#
. /etc/profile
APPNAME=GoNonRatAnnotationPipeline
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


echo "" >> $HOMEDIR/logs/status_summary.log
echo "Unprocessed Source Databases (count db):" >> $HOMEDIR/logs/status_summary.log

TODAY=`date +%Y%m%d`
zcat $HOMEDIR/data/${TODAY}_goa_human*.gaf.gz \
  | grep -v '^!' \
  | grep -v '^UniProtKB' | grep -v '^HGNC' | grep -v '^Ensembl' \
  | awk '{print $1}' \
  | sort | uniq -c       >> $HOMEDIR/logs/status_summary.log

mailx -s "[$SERVER] GO NonRat Annotation Pipeline" $EMAIL_LIST < $HOMEDIR/logs/status_summary.log
