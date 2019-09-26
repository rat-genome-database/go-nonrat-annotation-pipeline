    package edu.mcw.rgd;

    import edu.mcw.rgd.datamodel.ontology.Annotation;
    import edu.mcw.rgd.pipelines.PipelineRecord;
    import edu.mcw.rgd.pipelines.RecordProcessor;
    import edu.mcw.rgd.process.Utils;
    import org.apache.log4j.Logger;

    import java.util.*;

/**
 * @author mtutaj
 */
public class MAHDL extends RecordProcessor {

    protected final Logger logInsert = Logger.getLogger("insert");
    protected final Logger logUpdate = Logger.getLogger("update");

    private DAO dao;

    private Map<String, List<MAHAnnotData>> incomingAnnots = new HashMap<>();

    public MAHDL(DAO dao) {
        this.dao = dao;
    }

    @Override
    public void process(PipelineRecord pipelineRecord) throws Exception {
        MAHRecord rec = (MAHRecord) pipelineRecord;

        for( MAHAnnotData ad: rec.annotData ) {
            ad.key = computeAnnotKey(ad.incomingAnnot);
            ad.dbObjectId = rec.dbObjectID;
            List<MAHAnnotData> annots = incomingAnnots.get(ad.key);
            if( annots==null ) {
                annots = new ArrayList<>();
                incomingAnnots.put(ad.key, annots);
            }
            annots.add(ad);
        }
    }

    String computeAnnotKey( Annotation a ) {

        String key = a.getTermAcc()+"|";
        key += a.getAnnotatedObjectRgdId()+"|";
        key += a.getRefRgdId()+"|";
        key += a.getEvidence()+"|";
        key += Utils.defaultString(a.getQualifier())+"|";
        key += Utils.defaultString(a.getWithInfo());
        return key;
    }


    @Override
    public void onExit() throws Exception {

        // multi-thread version
        incomingAnnots.values().parallelStream().forEach( e -> {
            processAnnotBucket2(e);
        });
    }

    void processAnnotBucket2(List<MAHAnnotData> list) {
        try {
            processAnnotBucket(list);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    void processAnnotBucket(List<MAHAnnotData> list) throws Exception {
        // merge incoming annots: NOTES and XREF_SOURCE
        MAHAnnotData adata = list.get(0);
        Annotation merged = adata.incomingAnnot;
        Set<String> notes = new TreeSet<>();
        String notesStr = "";
        Set<String> xrefSource = new TreeSet<>();
        String xrefSourceStr = "";
        Set<String> xrefSourceWithPmids = new TreeSet<>();

        for( MAHAnnotData ad: list ) {
            Annotation a = ad.incomingAnnot;

            // merge xref sources
            String[] xrefCols = null;
            if( !Utils.isStringEmpty(a.getXrefSource()) ) {
                xrefCols = a.getXrefSource().split("[\\|\\,]");
                Collections.addAll(xrefSource, xrefCols);
            }
            String xrefSourceStrNew = Utils.concatenate(xrefSource,"|");
            if( xrefSourceStrNew.length()>4000 ) {
                getSession().incrementCounter("  TRUNCATED XREF_SOURCE (XREF_SOURCE LENGTH > 4000)", 1);
                // too long merged XREF_SOURCE -- emit existing annotation
                merged.setXrefSource(xrefSourceStr);
                merged.setNotes(notesStr);
                handleAnnot(merged, adata);

                // start new set of XREF_SOURCE, NOTES
                xrefSource.clear();
                Collections.addAll(xrefSource, xrefCols);
                notes.clear();
                xrefSourceWithPmids.clear();
            }

            if( !Utils.isStringEmpty(a.getNotes()) ) {
                String[] cols = a.getNotes().split("[\\|\\,]");
                Collections.addAll(notes, cols);
                // notes should be different from xrefs
                notes.removeAll(xrefSource);
            }

            if(a.getXrefSource().contains("PMID") ) {
                xrefSourceWithPmids.add("("+a.getXrefSource()+")");
            }

            xrefSourceStr = xrefSourceStrNew;
            notesStr = Utils.concatenate(notes, "|");
            if( !xrefSourceWithPmids.isEmpty() ) {
                notesStr += "  "+Utils.concatenate(xrefSourceWithPmids, ", ");
            }
        }

        // emit annot
        merged.setXrefSource(xrefSourceStr);
        merged.setNotes(notesStr);
        handleAnnot(merged, adata);
    }

    void handleAnnot(Annotation a, MAHAnnotData adata) throws Exception {

        // clear NOTES if they are the same as XREF_SOURCE
        if( !Utils.isStringEmpty(a.getNotes()) ) {
            if( Utils.stringsAreEqual(a.getXrefSource(), a.getNotes()) ) {
                a.setNotes(null);
            }
        }

        // get matching annot in RGD
        int annotKey = dao.getAnnotationKey(a);
        if( annotKey==0 ) {
            dao.insertAnnotation(a);
            logInsert.info(adata.db + ": FAKey:" + a.getKey() + " " + a.getTermAcc() + " term:" + a.getTerm() + " RGDID:" + a.getAnnotatedObjectRgdId() + " RefRGDID:" + a.getRefRgdId() + " " + adata.dbObjectId + " Ev:" + a.getEvidence() + " W:" + a.getWithInfo());
            getSession().incrementCounter("insertedAnnotCount", 1);
        }
        else {
            dao.updateAnnotationNotes(annotKey, a.getNotes());
            logUpdate.info(adata.db + ": FAKey:" + annotKey + " " + a.getTermAcc() + " RGDID:" + a.getAnnotatedObjectRgdId() + " RefRGDID:" + a.getRefRgdId() + " " + a.getEvidence() + " W:" + a.getWithInfo());
            getSession().incrementCounter("matchingAnnotCount", 1);
        }
    }
}
