# load GO Annotations for all species in RGD (except rat) into FULL_ANNOT table
#
. /etc/profile
APPNAME=GoNonRatAnnotationPipeline
HOMEDIR=/home/rgddata/pipelines/$APPNAME

SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
if [ "$SERVER" = "REED" ]; then
  EMAIL_LIST=rgd.devops@mcw.edu,jrsmith@mcw.edu
fi

cd $HOMEDIR

java -Dspring.config=$HOMEDIR/../properties/default_db2.xml \
  -Dlog4j.configuration=file://$HOMEDIR/properties/log4j.properties \
  -jar lib/$APPNAME.jar "$@" > run.log 2>&1

mailx -s "[$SERVER] GO NonRat Annotation Pipeline" $EMAIL_LIST < $HOMEDIR/logs/status_summary.log
