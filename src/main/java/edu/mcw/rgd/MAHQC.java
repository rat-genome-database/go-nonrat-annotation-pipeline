package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.pipelines.PipelineRecord;
import edu.mcw.rgd.pipelines.RecordProcessor;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author mtutaj
 * @since 1/7/15
 * QC module
 * <p>Note: as of May 13, 2016, created-date of all annotations is set to the current time stamp;
 * and incoming data created-date is no longer used by the pipeline
 */
public class MAHQC extends RecordProcessor {

    protected final Logger logStatus = Logger.getLogger("status");
    protected final Logger logUnmatched = Logger.getLogger("unmatched");
    protected final Logger logInactive = Logger.getLogger("inactive");
    protected final Logger logNoRatGene = Logger.getLogger("noRatGene");
    protected final Logger logNoGoTerm = Logger.getLogger("noGoTerm");
    protected final Logger logHighLevelGoTerm = Logger.getLogger("highLevelGoTerm");
    protected final Logger logCatalyticActivityIPIGoTerm = Logger.getLogger("catalyticActivityIPIGoTerm");

    static public Map<String,Integer> wrongEvidenceCounts = new HashMap<>();

    private MouseAndHumanGoAnnotationDAO dao;
    private Map<Integer,String> mapRgdIdStatus;
    private int internalRefRGDID;
    private int createdBy;
    private int isoRefRgdId;
    private int speciesTypeKey;
    private Map<String,String> sourceSubst;
    private Set<String> evidenceCodesToMakeRatAnnots;

    public void init(MouseAndHumanGoAnnotationDAO dao, Map<Integer,String> mapRgdIdStatus,
                 int internalRefRgdId, int createdBy, int issRefRgdId, int speciesTypeKey) {
        this.dao = dao;
        this.mapRgdIdStatus = mapRgdIdStatus;
        this.internalRefRGDID = internalRefRgdId;
        this.createdBy = createdBy;
        this.isoRefRgdId = issRefRgdId;
        this.speciesTypeKey = speciesTypeKey;
    }

    @Override
    public void process(PipelineRecord pipelineRecord) throws Exception {

        MAHRecord rec = (MAHRecord) pipelineRecord;
        rec.annotData.clear();

        // skip high level terms
        String termAcc=rec.fileLine[4];
        if( !dao.isForCuration(termAcc) ) {
            logHighLevelGoTerm.info(Arrays.toString(rec.fileLine));
            getSession().incrementCounter("highLevelGoTerm", 1);
            return;
        }

        // skip IPI annotations to catalytic activity terms
        String dBEvidenceCode = rec.fileLine[6];
        if( dBEvidenceCode.equals("IPI") && dao.isCatalyticActivityTerm(termAcc) ) {
            getSession().incrementCounter("IPIAnnotToCatalyticActivityTerm", 1);
            logCatalyticActivityIPIGoTerm.info(Arrays.toString(rec.fileLine));
            return;
        }

        List<Gene> genes = qcGenes(rec);

        Collection<Gene> validGenes = validateGeneStatus(genes, rec.dbObjectID, rec.dbName);

        if (validGenes.isEmpty()) {
            logUnmatched.info(rec.dbName+": "+rec.dbObjectID);
            getSession().incrementCounter("unmatchedCounter", 1);
            return; // no match either -- continue to the next file line
        }

        for( Gene gene: validGenes ) {

            if( gene.getSpeciesTypeKey()!=speciesTypeKey ) {
                logStatus.warn("unexpected species, line ignored: "+rec.dbObjectID);
                continue;
            }
            getSession().incrementCounter("MATCH BY "+rec.dbName, 1);

            int geneRgdId = gene.getRgdId();
            createRatAnnotations(geneRgdId, rec, dBEvidenceCode);
            loadIntoFULL_ANNOT(rec, rec.fileLine[14], dBEvidenceCode, rec.fileLine[7], rec.fileLine[5], internalRefRGDID, geneRgdId, rec.fileLine);
        }
    }

    List<Gene> qcGenes(MAHRecord rec) throws Exception {
        // match by $DB_Object_ID
        String dBObjectID = rec.fileLine[1];
        int xdbKey = 0;
        int xdbKeySecondary = 0;
        if( rec.dbName.equals("UniProtKB") ) {
            xdbKey = XdbId.XDB_KEY_UNIPROT;
            xdbKeySecondary = XdbId.XDB_KEY_UNIPROT_SECONDARY;
        } else if( rec.dbName.equals("HGNC") ) {
            xdbKey = XdbId.XDB_KEY_HGNC;
            if( !dBObjectID.startsWith("HGNC:") ) {
                dBObjectID = "HGNC:" + dBObjectID;
            }
        } else if( rec.dbName.equals("Ensembl") ) {
            xdbKey = XdbId.XDB_KEY_ENSEMBL_GENES;
        } else if( rec.dbName.equals("MGI") ) {
            xdbKey = XdbId.XDB_KEY_MGD;
        } else if( rec.dbName.equals("RGD") ) {
            // special handling for chinchilla
            rec.dbObjectID = rec.fileLine[1];
            Gene gene = new Gene();
            gene.setRgdId(Integer.parseInt(rec.dbObjectID));
            gene.setSymbol(rec.fileLine[2]);
            gene.setName(rec.fileLine[9]);
            gene.setSpeciesTypeKey(speciesTypeKey);
            List<Gene> genes = new ArrayList<>(1);
            genes.add(gene);
            return genes;
        }

        List<Gene> genes = dao.getGenesByXdbId(xdbKey, dBObjectID);
        if( genes.isEmpty() ) {
            // no match for $DB_Object_ID, try same id as a secondary xdb key
            if (xdbKeySecondary != 0) {
                genes = dao.getGenesByXdbId(xdbKeySecondary, dBObjectID);
            }
        }
        if( genes.isEmpty() ) {
            // no match for $DB_Object_ID, try to match by $Gene_Product_Form_Id
            String dBObjectID2 = readAltDbObjectId(rec);
            if( dBObjectID2!=null ) {
                dBObjectID = dBObjectID2;
                genes = dao.getGenesByXdbId(xdbKey, dBObjectID);
            }
        }
        rec.dbObjectID = dBObjectID;
        return genes;
    }

    Collection<Gene> validateGeneStatus(List<Gene> genes, String dBObjectID, String dbName) throws Exception {

        Map<Integer, Gene> genesValidated = new HashMap<>();

        for( Gene gene: genes ) {
            String geneStatus = mapRgdIdStatus.get(gene.getRgdId());
            if( geneStatus.equals("ACTIVE") ) {
                genesValidated.put(gene.getRgdId(), gene);
            }
            else {

                int newRGDIDHistory = dao.getRgdIdFromHistory(gene.getRgdId());
                if (newRGDIDHistory != 0 ) {
                    logInactive.info(dbName + ": Retired FAKey: DBID:" + dBObjectID
                            + " OLD_RGD_ID:" + gene.getRgdId() + " NEW_RGD_ID:" + newRGDIDHistory);

                    genesValidated.put(newRGDIDHistory, dao.getGene(newRGDIDHistory));
                }
                genesValidated.remove(gene.getRgdId());
                logInactive.info(dbName + ": RGD_ID:" + gene.getRgdId() + " DBID:" + dBObjectID);
                getSession().incrementCounter("inactiveCounter", 1);
            }
        }
        return genesValidated.values();
    }

    void createRatAnnotations(int geneRgdId, MAHRecord rec, String dBEvidenceCode) throws Exception {

        List<Gene> orthologs = dao.getRatOrthologs(geneRgdId);
        if (orthologs.isEmpty()) {
            logNoRatGene.info(rec.dbName + ": RGDID:" + geneRgdId);
            getSession().incrementCounter("noRatGeneCounter", 1);
        }
        else {
            for(Gene ortholog: orthologs ) {
                if( ortholog.getSpeciesTypeKey() != SpeciesType.RAT ) {
                    continue;
                }

                if( getEvidenceCodesToMakeRatAnnots().contains(dBEvidenceCode) ) {

                    int rGDIDOrthologous = ortholog.getRgdId();
                    String rGDID4withInfoField = "RGD:" + geneRgdId;
                    String notes = Utils.isStringEmpty(rec.fileLine[7]) ? rec.fileLine[5] : rec.fileLine[7];

                    loadIntoFULL_ANNOT(rec, "RGD", "ISO", rGDID4withInfoField, notes, isoRefRgdId, rGDIDOrthologous, rec.fileLine);
                } else {
                    incrementWrongEvidenceCount(dBEvidenceCode);
                }
            }
        }
    }

    synchronized void incrementWrongEvidenceCount(String evidence) {
        Integer count = wrongEvidenceCounts.get(evidence);
        if( count==null ) {
            count = 1;
        } else {
            count++;
        }
        wrongEvidenceCounts.put(evidence, count);
    }

    String readAltDbObjectId(MAHRecord rec) {
        String dBObjectID = null;
        // deal with GAF versions 1.0 and 2.0 (rene lopez)
        if (rec.fileLine.length == 17) {
            String a_DbObjectId[] = rec.fileLine[16].split(":");
            if (a_DbObjectId.length >= 2) {
                dBObjectID = a_DbObjectId[1];
            }
        }
        return dBObjectID;
    }

    public void loadIntoFULL_ANNOT(MAHRecord rec, String dataSourceField, String evidenceField, String withInfoField,
            String notesField, int refRGDIDField, int rGDID, String line[]) throws Exception{

        // skip if no reference
        if( refRGDIDField==0 ) {
            return;
        }

        // skip self-referencing annotations: where WITH_INFO = annotated_object_rgd_id
        if( withInfoField.equals("RGD:"+rGDID) ) {
            getSession().incrementCounter("skippedSelfRefAnnots", 1);
            return;
        }

        MAHAnnotData annotData = new MAHAnnotData();
        annotData.db = rec.dbName;

        Annotation annot = new Annotation();

        String qualifier=line[3];
        String gOID=line[4];
        String dBReference=line[5];
        String aspect=line[8];

        // read term name from ont_terms table
        Term term = dao.getTermByAccId(gOID);
        if( term!=null ) {
            annot.setTerm(term.getTerm());
        }
        else {
            logNoGoTerm.warn("go term "+gOID+" is not found in RGD database!");
            return;
        }

        // populate and validate data source field
        String dataSource = getSourceSubst().get(dataSourceField);
        if( dataSource==null ) {
            dataSource = dataSourceField;
        } else {
            getSession().incrementCounter("DATA_SRC substitutions", 1);
        }

        annot.setAnnotatedObjectRgdId(rGDID);
        annot.setRgdObjectKey(RgdId.OBJECT_KEY_GENES);
        annot.setDataSrc(dataSource);//assignedBy for annotation, RGD for Rat ISO

        // load gene symbol and gene name
        Gene gene = dao.getGene(rGDID);
        if( gene!=null ) {
            annot.setObjectSymbol(gene.getSymbol());
            annot.setObjectName(gene.getName());
        }

        annot.setRefRgdId(refRGDIDField);// 2290270 for mouse, 2290271 for human during annotation, 1624291 for Rat ISO
        annot.setEvidence(evidenceField);//evidenceCode for annotation, ISO for Rat ISO
        annot.setWithInfo(withInfoField);//with for annotation, RGDID of the mouse/human gene for Rat ISO
        annot.setAspect(aspect);
        annot.setNotes(notesField); //dBReference for annotation and for Rat ISO if $with from file is null, with for Rat ISO if $with from file is not null
        annot.setQualifier(qualifier);

        annot.setTermAcc(gOID);
        annot.setCreatedBy(createdBy);
        annot.setLastModifiedBy(createdBy);
        annot.setXrefSource(dBReference);

        annotData.incomingAnnot = annot;
        rec.annotData.add(annotData);
    }

    public void setSourceSubst(Map<String,String> sourceSubst) {
        this.sourceSubst = sourceSubst;
    }

    public Map<String,String> getSourceSubst() {
        return sourceSubst;
    }

    public void setEvidenceCodesToMakeRatAnnots(Set<String> evidenceCodesToMakeRatAnnots) {
        this.evidenceCodesToMakeRatAnnots = evidenceCodesToMakeRatAnnots;
    }

    public Set<String> getEvidenceCodesToMakeRatAnnots() {
        return evidenceCodesToMakeRatAnnots;
    }
}
