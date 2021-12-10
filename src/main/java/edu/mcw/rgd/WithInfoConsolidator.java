package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.Utils;

import java.util.*;

public class WithInfoConsolidator {

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
                if( adList.size()==1 ) {
                    return;
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

    int consolidateByMergeInfo( String mergedWithInfo, List<MAHAnnotData> adList ) {

        final int MAX_WITH_INFO_LEN = 1700;

        // set the merged WITH_INFO to first incoming entry/entries, and delete the rest
        int adIndex = 0;
        while( mergedWithInfo.length()>MAX_WITH_INFO_LEN ) {
            int barPos = mergedWithInfo.lastIndexOf('|', MAX_WITH_INFO_LEN-1);
            String mergedWithInfoFragment = mergedWithInfo.substring(0, barPos);
            mergedWithInfo = mergedWithInfo.substring(barPos+1);
            adList.get(adIndex++).incomingAnnot.setWithInfo(mergedWithInfoFragment);
        }
        adList.get(adIndex).incomingAnnot.setWithInfo(mergedWithInfo);

        int deleteCount = 0;
        for( int i=1+adIndex; i<adList.size(); i++ ) {
            MAHAnnotData ad = adList.get(i);
            synchronized (ad.parentList) {
                ad.parentList.remove(ad);
            }
            deleteCount++;
        }
        return deleteCount;
    }
}
