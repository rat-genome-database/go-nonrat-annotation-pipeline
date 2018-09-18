# load mouse and human GO Annotations into FULL_ANNOT table
#
. /etc/profile
HOMEDIR=/home/rgddata/pipelines/mouseandhumanGOAnnotationPipeline

$HOMEDIR/_run.sh

echo "Unprocessed Source Databases (count db):"
TODAY=`date +%Y%m%d`
zcat $HOMEDIR/data/${TODAY}_goa_human*.gaf.gz \
  | grep -v '^!' \
  | grep -v '^UniProtKB' | grep -v '^HGNC' | grep -v '^Ensembl' \
  | awk '{print $1}' \
  | sort | uniq -c
