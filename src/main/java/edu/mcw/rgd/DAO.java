package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mtutaj
 * @since Sep 1, 2010
 * all database related stuff should be here
 */
public class DAO  {

    protected final Logger logDelete = Logger.getLogger("delete");

    AnnotationDAO annotationDAO = new AnnotationDAO();
    GeneDAO geneDAO = new GeneDAO();
    OntologyXDAO ontologyDAO = new OntologyXDAO();
    RGDManagementDAO managementDAO = new RGDManagementDAO();
    XdbIdDAO xdbIdDAO = new XdbIdDAO();

    public String getConnectionInfo() {
        return geneDAO.getConnectionInfo();
    }

    /**
     * Returns a Gene based on an rgd id
     * @param rgdId rgd id
     * @return Gene object for given rgd id
     * @throws Exception thrown when there is no gene with such rgd id
     */
    synchronized public Gene getGene(int rgdId) throws Exception {

        // try to get gene object from cache first
        Gene gene = _geneCache.get(rgdId);

        if( gene==null ) {
            // gene is not in a cache yet -- get it from database and put it into the cache
            try {
                gene =  geneDAO.getGene(rgdId);
                if( gene!=null )
                    _geneCache.put(rgdId, gene);
            } catch( GeneDAO.GeneDAOException e ) {
                // there is no gene with such an rgd id
                gene = null;
            }
        }
        return gene;
    }
    private static HashMap<Integer, Gene> _geneCache = new HashMap<>(30007);

    /**
     * gets term by term accession id
     * @param termAcc term accession id
     * @return Term object or null if acc id is not valid
     * @throws Exception when something unexpected happens in spring framework
     */
    synchronized public Term getTermByAccId(String termAcc) throws Exception {

        Term term = _termCache.get(termAcc);
        if( term==null ) {
            term = ontologyDAO.getTermByAccId(termAcc);
            _termCache.put(termAcc, term);
        }
        return term;
    }
    private static Map<String, Term> _termCache = new HashMap<>(30007);


    public Map<Integer,String> getStatusForGeneRgdIds() throws Exception {
        final Map<Integer,String> map = new HashMap<>(22223);
        for( RgdId id: managementDAO.getRgdIds(RgdId.OBJECT_KEY_GENES) ) {
            map.put(id.getRgdId(), id.getObjectStatus());
        }
        return map;
    }

    public int getCountOfAnnotationForRefRgdId(int refRgdId) throws Exception {
        return annotationDAO.getCountOfAnnotationsByReference(refRgdId);
    }

     synchronized public List<Gene> getRatOrthologs(int srcRgdId) throws Exception {

        List<Gene> orthologs = _orthologCache.get(srcRgdId);
        if( orthologs==null ) {
            orthologs = geneDAO.getActiveOrthologs(srcRgdId, SpeciesType.RAT);
            _orthologCache.put(srcRgdId, orthologs);
        }
        return orthologs;
    }
    private static HashMap<Integer, List<Gene>> _orthologCache = new HashMap<>(30007);

    /**
     * get new_rgd_id from RGD_ID_HISTORY for given rgd_id
     * @param rgdId rgd id of an object to be examined
     * @return new_rgd_id taken from RGD_ID_HISTORY or 0, if there is no history record
     * @throws Exception when unexpected error in spring framework occurs
     */
    public int getRgdIdFromHistory(int rgdId) throws Exception {

        return managementDAO.getActiveRgdIdFromHistory(rgdId);
    }

    public List<Gene> getGenesByXdbId(int xdbKey, String accId) throws Exception {

        String key = xdbKey+"|"+accId;

        synchronized(_genesCache2) {
            // try to get data from cache first
            List<Gene> genes = _genesCache2.get(key);
            if (genes == null) {
                // not in cache -- fetch data from db
                genes = xdbIdDAO.getGenesByXdbId(xdbKey, accId);
                _genesCache2.put(key, genes);
            }
            return genes;
        }
    }
    private static final HashMap<String, List<Gene>> _genesCache2 = new HashMap<>(10007);

    /**
     * get annotation key by a list of values that comprise unique key:
     * TERM_ACC+ANNOTATED_OBJECT_RGD_ID+REF_RGD_ID+EVIDENCE+WITH_INFO+QUALIFIER+XREF_SOURCE
     * @param annot Annotation object with the following fields set: TERM_ACC+ANNOTATED_OBJECT_RGD_ID+REF_RGD_ID+EVIDENCE+WITH_INFO+QUALIFIER+XREF_SOURCE
     * @return value of annotation key or 0 if there is no such annotation
     * @throws Exception on spring framework dao failure
     */
    public int getAnnotationKey(Annotation annot) throws Exception {

        return annotationDAO.getAnnotationKey(annot);
    }

    public Annotation getAnnotation(int key) throws Exception {
        return annotationDAO.getAnnotation(key);
    }

    /**
     * Insert new annotation into FULL_ANNOT table; full_annot_key, creation_date and last_modified_date will be set
     *
     * @param annot Annotation object representing column values
     * @throws Exception
     */
    public void insertAnnotation(Annotation annot) throws Exception{

        try {
            annotationDAO.insertAnnotation(annot);
        } catch(Exception e) {
            Logger logStatus = Logger.getLogger("status");
            logStatus.warn("INSERT PROBLEM for "+annot.dump("|"));
            throw e;
        }
    }

    public int updateAnnotEx(int key, String notes, String annotExt, String geneProductFormId, Date origCreatedDate) throws Exception {
        String sql = "UPDATE full_annot SET notes=?,annotation_extension=?,gene_product_form_id=?,original_created_date=?,last_modified_date=SYSDATE WHERE full_annot_key=?";
        return annotationDAO.update(sql, notes, annotExt, geneProductFormId, origCreatedDate, key);
    }

    public int updateLastModifiedDateForAnnotation(int annotKey) throws Exception{
        return annotationDAO.updateLastModified(annotKey);
    }

    /**
     * delete annotations older than passed in date;
     * deleted annotations are logged in the file 'deleted.log';
     * if the net annot drop is greater than 5% threshold, then abort the deletions and report it
     * @return number of rows affected
     * @throws Exception on spring framework dao failure
     */
    public int deleteAnnotations(int createdBy, Date dt, Logger logStatus, String deleteThresholdStr, int refRgdId, int initialAnnotCount, int speciesTypeKey) throws Exception{

        // extract delete threshold in percent
        int percentPos = deleteThresholdStr.indexOf('%');
        int deleteThreshold = Integer.parseInt(deleteThresholdStr.substring(0, percentPos));

        int currentAnnotCount = getCountOfAnnotationsByReferenceAndSpecies(refRgdId, speciesTypeKey);
        List<Annotation> annotsForDelete = annotationDAO.getAnnotationsModifiedBeforeTimestamp(createdBy, dt, refRgdId, speciesTypeKey);
        List<Integer> fullAnnotKeys = new ArrayList<>(annotsForDelete.size());
        for( Annotation a: annotsForDelete ) {
            logDelete.info(a.dump("|"));
            fullAnnotKeys.add(a.getKey());
        }
        int annotsForDeleteCount = annotsForDelete.size();
        int annotsForDeleteThreshold = (deleteThreshold * currentAnnotCount) / 100; // 5% delete threshold

        int newAnnotCount = currentAnnotCount - annotsForDeleteCount;
        if( initialAnnotCount - newAnnotCount > annotsForDeleteThreshold ) {

            logStatus.warn(" STALE ANNOTATIONS DELETE THRESHOLD ("+deleteThresholdStr+") -- "+annotsForDeleteThreshold);
            logStatus.warn(" STALE ANNOTATIONS TAGGED FOR DELETE     -- "+annotsForDeleteCount);
            logStatus.warn(" STALE ANNOTATIONS DELETE THRESHOLD ("+deleteThresholdStr+") EXCEEDED -- no annotations deleted");
            return 0;
        }
        return annotationDAO.deleteAnnotations(fullAnnotKeys);
    }

    public int getCountOfAnnotationsByReferenceAndSpecies(int refRgdId, int speciesTypeKey) throws Exception {

        String query = "SELECT COUNT(*) "+
                "FROM full_annot a,rgd_ids r "+
                "WHERE ref_rgd_id=? AND r.species_type_key=? AND annotated_object_rgd_id=rgd_id AND r.object_status='ACTIVE'";
        return annotationDAO.getCount(query, refRgdId, speciesTypeKey);
    }

    /**
     * Check if a term can be used for curation.
     * @param termAcc term ACC id
     * @return true if the term doesn't have a "Not4Curation" synonym
     * @throws Exception if something wrong happens in spring framework
     */
    public boolean isForCuration(String termAcc) throws Exception {

        Boolean result = _isForCurationCache.get(termAcc);
        if( result==null ) {
            result = ontologyDAO.isForCuration(termAcc);
            _isForCurationCache.put(termAcc, result);
        }
        return result;
    }
    Map<String,Boolean> _isForCurationCache = new ConcurrentHashMap<>();

    /**
     * check if a given term is a catalytic activity term GO:0003824, or a child of catalytic activity term
     * @param termAcc accession id of the term in question
     * @return true if the term is related to catalytic activity
     * @throws Exception if something wrong happens in spring framework
     */
    public boolean isCatalyticActivityTerm(String termAcc) throws Exception {
        final String catalyticActivityAccId = "GO:0003824";
        return termAcc.equals(catalyticActivityAccId) || ontologyDAO.isDescendantOf(termAcc, catalyticActivityAccId);
    }

    public List<Annotation> getManualGoAnnotsForChinchilla() throws Exception {
        String sql = "SELECT * FROM full_annot WHERE created_by NOT IN(67,192) AND term_acc LIKE 'GO:%' AND "+
            "EXISTS( SELECT 1 FROM rgd_ids r WHERE annotated_object_rgd_id=rgd_id AND object_status='ACTIVE' AND species_type_key=4)";
        return annotationDAO.executeAnnotationQuery(sql);
    }

    public String getSpeciesShortName(int speciesTypeKey) throws Exception {
        String sql = "SELECT MAX(short_name) FROM species_types WHERE species_type_key=?";
        return annotationDAO.getStringResult(sql, speciesTypeKey);
    }
}
