package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author mtutaj
 */
public class MAHDL {

    protected final Logger logInsert = Logger.getLogger("insert");
    protected final Logger logUpdated = Logger.getLogger("updated");
    protected final Logger logUpToDate = Logger.getLogger("upToDate");

    private DAO dao;

    private Map<String, List<MAHAnnotData>> incomingAnnots = new HashMap<>();

    public MAHDL(DAO dao) {
        this.dao = dao;
    }

    public void process(MAHRecord rec, CounterPool counters) {

        for( MAHAnnotData ad: rec.annotData ) {
            ad.key = computeAnnotKey(ad.incomingAnnot);
            ad.dbObjectId = rec.dbObjectID;
            synchronized (this) {
                List<MAHAnnotData> annots = incomingAnnots.get(ad.key);
                if (annots == null) {
                    annots = new ArrayList<>();
                    incomingAnnots.put(ad.key, annots);
                }
                annots.add(ad);
            }
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

    public void postProcess(CounterPool counters) {

        incomingAnnots.values().parallelStream().forEach( list -> {
            try {
                processAnnotBucket(list, counters);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void processAnnotBucket(List<MAHAnnotData> list, CounterPool counters) throws Exception {
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
                counters.increment("  TRUNCATED XREF_SOURCE (XREF_SOURCE LENGTH > 4000)");
                // too long merged XREF_SOURCE -- emit existing annotation
                merged.setXrefSource(xrefSourceStr);
                merged.setNotes(notesStr);
                handleAnnot(merged, adata, counters);

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
        handleAnnot(merged, adata, counters);
    }

    void handleAnnot(Annotation a, MAHAnnotData adata, CounterPool counters) throws Exception {

        // clear NOTES if they are the same as XREF_SOURCE
        if( !Utils.isStringEmpty(a.getNotes()) ) {
            if( Utils.stringsAreEqual(a.getXrefSource(), a.getNotes()) ) {
                a.setNotes(null);
            }
        }

        // get matching annot in RGD
        int annotKey = dao.getAnnotationKey(a);
        if( annotKey==0 ) {
            String msg = adata.db + " " + a.getTermAcc() + " [" + a.getTerm() + "] RGD:" + a.getAnnotatedObjectRgdId() + " RefRGD:" + a.getRefRgdId() + " " + adata.dbObjectId + " E:"+a.getEvidence();
            if( !Utils.isStringEmpty(a.getWithInfo()) ) {
                msg += " W:" + a.getWithInfo();
            }
            if( a.getQualifier()!=null ) {
                msg += " Q:" + a.getQualifier();
            }
            if( a.getXrefSource()!=null ) {
                msg += " XREFSRC:" + a.getXrefSource();
            }
            logInsert.info(msg);
            dao.insertAnnotation(a);
            counters.increment("insertedAnnotCount");
        }
        else {

            // check if you need to update notes, annot ext
            Annotation annotInRgd = dao.getAnnotation(annotKey);
            boolean changed = !Utils.stringsAreEqual(annotInRgd.getNotes(), a.getNotes())
                    || !Utils.stringsAreEqual(annotInRgd.getAnnotationExtension(), a.getAnnotationExtension())
                    || !Utils.stringsAreEqual(annotInRgd.getGeneProductFormId(), a.getGeneProductFormId())
                    || !Utils.datesAreEqual(annotInRgd.getOriginalCreatedDate(), a.getOriginalCreatedDate());

            if( changed ) {
                String msg = adata.db + ": FAK:" + annotKey + " " + a.getTermAcc() + " RGD:" + a.getAnnotatedObjectRgdId() + " RefRGD:" + a.getRefRgdId() + " " + a.getEvidence() + " W:" + a.getWithInfo();
                if( !Utils.stringsAreEqual(annotInRgd.getAnnotationExtension(), a.getAnnotationExtension()) ) {
                    msg += "\n   ANNOT_EXT  OLD["+Utils.NVL(annotInRgd.getAnnotationExtension(),"")+"]  NEW["+a.getAnnotationExtension()+"]";
                }
                if( !Utils.stringsAreEqual(annotInRgd.getGeneProductFormId(), a.getGeneProductFormId()) ) {
                    msg += "\n   GENE_FORM  OLD["+Utils.NVL(annotInRgd.getGeneProductFormId(),"")+"]  NEW["+a.getGeneProductFormId()+"]";
                }
                if( !Utils.datesAreEqual(annotInRgd.getOriginalCreatedDate(), a.getOriginalCreatedDate()) ) {
                    msg += "\n   ORIG CREATED DATE  OLD["+dumpDate(annotInRgd.getOriginalCreatedDate())+"]  NEW["+dumpDate(a.getOriginalCreatedDate())+"]";
                    counters.increment("updatedOrigCreatedDate");
                }
                if( !Utils.stringsAreEqual(annotInRgd.getNotes(), a.getNotes()) ) {
                    msg += "\n   NOTES  OLD["+Utils.NVL(annotInRgd.getNotes(),"")+"]  NEW["+a.getNotes()+"]";
                }
                logUpdated.info(msg);

                dao.updateAnnotEx(annotKey, a.getNotes(), a.getAnnotationExtension(), a.getGeneProductFormId(), a.getOriginalCreatedDate());
                counters.increment("updatedAnnotCount");
            } else {
                dao.updateLastModifiedDateForAnnotation(annotKey);
                logUpToDate.info(adata.db + ": FAK:" + annotKey + " " + a.getTermAcc() + " RGD:" + a.getAnnotatedObjectRgdId() + " RefRGD:" + a.getRefRgdId() + " " + a.getEvidence() + " W:" + a.getWithInfo());
                counters.increment("matchingAnnotCount");
            }
        }
    }

    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    synchronized String dumpDate(Date dt) {
        return dt==null ? "" : dateFormat.format(dt);
    }
}
