package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.Utils;

import java.util.*;

public class WithInfoConsolidator {

    static final int MAX_WITH_INFO_LEN = 1700;

    static public void run(List<MAHRecord> records, CounterPool counters) {

        WithInfoConsolidator consolidator = new WithInfoConsolidator();
        consolidator.mergeWithInfo(records, counters);

    }

    ///////////////////////////////
    // merge annots having all fields the same but different WITH_INFO

    String computeAnnotKey( Annotation a ) {

        String key = a.getTermAcc()+"|";
        key += a.getAnnotatedObjectRgdId()+"|";
        key += a.getRefRgdId()+"|";
        key += a.getEvidence()+"|";
        key += Utils.defaultString(a.getQualifier())+"|";
        key += Utils.defaultString(a.getXrefSource())+"|";
        key += Utils.defaultString(a.getDataSrc())+"|";
        key += Utils.defaultString(a.getNotes());
        if( Utils.isStringEmpty(a.getWithInfo()) )
            key += " EMPTY";
        return key;
    }

    public void mergeWithInfo(List<MAHRecord> list, CounterPool counters) {

        Map<String, List<MAHAnnotData>> annotMap = new HashMap<>();

        for( MAHRecord rec: list ) {
            for (MAHAnnotData ad : rec.annotData) {
                ad.key = computeAnnotKey(ad.incomingAnnot);
                ad.dbObjectId = rec.dbObjectID;
                ad.parentList = rec.annotData;
                synchronized (this) {
                    List<MAHAnnotData> annots = annotMap.get(ad.key);
                    if (annots == null) {
                        annots = new ArrayList<>();
                        annotMap.put(ad.key, annots);
                    }
                    annots.add(ad);
                }
            }
        }

        annotMap.values().parallelStream().forEach( adList -> {
            try {
                // singleton bucket with WITH_INFO already short enough: nothing to consolidate or split
                if( adList.size()==1 ) {
                    String wi = adList.get(0).incomingAnnot.getWithInfo();
                    if( wi==null || wi.length()<=MAX_WITH_INFO_LEN ) {
                        return;
                    }
                }

                // merge WITH_INFO
                TreeSet<String> withInfoSet = new TreeSet<>();
                for( MAHAnnotData mah: adList ) {
                    String[] withInfos = mah.incomingAnnot.getWithInfo().split("[|,]");
                    for( String withInfo: withInfos ) {
                        withInfoSet.add(withInfo);
                    }
                }
                String mergedWithInfo = Utils.concatenate(withInfoSet,"|");

                int deleteCount = consolidateByMergeInfo(mergedWithInfo, adList);

                counters.add("with_info merges", deleteCount);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        });

        // remove incoming lines that have been merged with other lines
        Iterator<MAHRecord> it = list.iterator();
        while( it.hasNext() ) {
            MAHRecord rec = it.next();
            if( rec.annotData.isEmpty() ) {
                it.remove();
                counters.increment("incoming lines removed");
            }
        }
    }

    int consolidateByMergeInfo( String mergedWithInfo, List<MAHAnnotData> adList ) throws CloneNotSupportedException {

        // break merged WITH_INFO into fragments, each <= MAX_WITH_INFO_LEN, preferring '|' boundaries
        List<String> fragments = new ArrayList<>();
        while( mergedWithInfo.length()>MAX_WITH_INFO_LEN ) {
            int barPos = mergedWithInfo.lastIndexOf('|', MAX_WITH_INFO_LEN-1);
            if( barPos <= 0 ) {
                // no '|' separator within the limit — hard-cut at MAX_WITH_INFO_LEN
                fragments.add(mergedWithInfo.substring(0, MAX_WITH_INFO_LEN));
                mergedWithInfo = mergedWithInfo.substring(MAX_WITH_INFO_LEN);
            } else {
                fragments.add(mergedWithInfo.substring(0, barPos));
                mergedWithInfo = mergedWithInfo.substring(barPos+1);
            }
        }
        fragments.add(mergedWithInfo);

        // assign each fragment to a MAHAnnotData slot; clone & append when more slots are needed
        for( int i=0; i<fragments.size(); i++ ) {
            if( i >= adList.size() ) {
                MAHAnnotData template = adList.get(0);
                MAHAnnotData clone = new MAHAnnotData();
                clone.incomingAnnot = (Annotation) template.incomingAnnot.clone();
                clone.db = template.db;
                clone.key = template.key;
                clone.dbObjectId = template.dbObjectId;
                clone.parentList = template.parentList;
                synchronized (template.parentList) {
                    template.parentList.add(clone);
                }
                adList.add(clone);
            }
            adList.get(i).incomingAnnot.setWithInfo(fragments.get(i));
        }

        // remove leftover adList entries (fewer fragments than slots)
        int deleteCount = 0;
        for( int i=fragments.size(); i<adList.size(); i++ ) {
            MAHAnnotData ad = adList.get(i);
            synchronized (ad.parentList) {
                ad.parentList.remove(ad);
            }
            deleteCount++;
        }
        return deleteCount;
    }
}
