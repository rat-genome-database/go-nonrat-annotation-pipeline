package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.pipelines.*;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.util.Date;
import java.util.*;

/**
 * - download and process mouse, human and dog GO annotations from GO consortium
 * - generates inferred rat annotations from GO manual chinchilla annotations
 * <p>
 * Logic for mouse-human-dog: <ul>
 * <li>created-date of incoming annotations that are inserted into db is set to SYSDATE
 * <li>last-modified-date is set to SYSDATE every time the annotation is processed through the pipeline
 * <li>created-date is refreshed (set to SYSDATE) for IEA annotations with created-date older than 9 months
 * <li>NOTES is set to ids that are different from the one in XREF_SOURCE
 * <li>NOTES and XREF_SOURCE fields are combined for similar annotations
 * </ul>
 */
public class MouseAndHumanGoAnnotationPipeline {

    private String remoteDir;
    private String localDir;
    private List<String> mgiFiles;
    private List<String> goaHumanFiles;
    private List<String> goaHumanDbSources;
    private List<String> goaMouseDbSources;
    private List<String> goaDogFiles;
    private List<String> goaDogDbSources;
    private int goaHumanRefRgdId;
    private int mgiRefRgdId;
    private int goaDogRefRgdId;
    private int issRefRgdId;
    private int createdBy;
    private String version;
    private int pipelineQueueSize;
    private int qcThreadCount;

    MouseAndHumanGoAnnotationDAO dao = new MouseAndHumanGoAnnotationDAO();
    Map<Integer,String> mapRgdIdStatus;

    protected final Logger logStatus = Logger.getLogger("status");
    protected final Logger logException = Logger.getLogger("exception");
    private MAHQC qc;
    private String staleAnnotDeleteThreshold;
    private MAHParser parser;

    public static void main(String args[]) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        MouseAndHumanGoAnnotationPipeline loader = (MouseAndHumanGoAnnotationPipeline) (bf.getBean("loader"));
        loader.logStatus.info("--- "+loader.getVersion()+" ---");

        for( int i=0; i<args.length; i++ ) {
            switch(args[i]) {
                case "--qcThreadCount":
                    int qcThreadCount = Integer.parseInt(args[++i]);
                    if( qcThreadCount>0 ) {
                        loader.setQcThreadCount(qcThreadCount);
                    }
            }
        }
        loader.logStatus.info(" -- QC_THREAD_COUNT = "+loader.getQcThreadCount());

        loader.run();
    }

    public void processFile(List<String> fileNames, List<String> fromDatabases, int internalRefRGDID, int speciesTypeKey) throws Exception{

        parser.init(fileNames, fromDatabases, speciesTypeKey, dao);
        qc.init(dao, mapRgdIdStatus, internalRefRGDID, createdBy, issRefRgdId, speciesTypeKey);
        MAHDL dl = new MAHDL(dao);

        PipelineManager manager = new PipelineManager();
        manager.addPipelineWorkgroup(parser, "PP", 1, getPipelineQueueSize());
        manager.addPipelineWorkgroup(qc, "QC", getQcThreadCount(), getPipelineQueueSize());
        manager.addPipelineWorkgroup(dl, "DL", 1, getPipelineQueueSize());

        // because we are doing annotation QC and loading in parallel thread, conflicts could happen
        // resulting in an attempt to insert duplicate annotations;
        // we do allow for up-to 100000 duplicate annotations to be resolved later
        manager.getSession().setAllowedExceptions(100000);

        // violations of unique key during inserts of annotations will be handled silently,
        // without writing anything to the logs
        manager.getSession().registerUserException(new String[]{
                "FULL_ANNOT_MULT_UC", "DataIntegrityViolationException", "SQLIntegrityConstraintViolationException"});

        manager.run();

        dumpStats(manager, speciesTypeKey);

        logStatus.info("===  "+SpeciesType.getCommonName(speciesTypeKey)+" OK  ===");
    }

    public void run() throws Exception{

        Date pipelineStartTime = new Date();

        logStatus.info("evidence codes to make inferred rat annotations: "
                +Utils.concatenate(qc.getEvidenceCodesToMakeRatAnnots(), ", ", "\'"));

        mapRgdIdStatus = dao.getStatusForGeneRgdIds();

        // show current counts
        dumpCountsForRefRgdIds();

        // process the data
        downloadAndProcessFiles(getGoaHumanDbSources(), getGoaHumanRefRgdId(), getGoaHumanFiles(), SpeciesType.HUMAN);
        downloadAndProcessFiles(getGoaMouseDbSources(), getMgiRefRgdId(), getMgiFiles(), SpeciesType.MOUSE);
        downloadAndProcessFiles(getGoaDogDbSources(), getGoaDogRefRgdId(), getGoaDogFiles(), SpeciesType.DOG);
        processFile(null, null, 0, SpeciesType.CHINCHILLA);

        // delete annotations not updated/inserted by the pipeline
        deleteStaleAnnotations(pipelineStartTime);

        // show current counts
        dumpCountsForRefRgdIds();

        logStatus.info("evidence codes not used to make rat annotations:");
        for( Map.Entry<String,Integer> entry: MAHQC.wrongEvidenceCounts.entrySet() ) {
            logStatus.info("  "+entry.getKey()+": "+entry.getValue());
        }

        // show total elapsed time
        long startTime = pipelineStartTime.getTime();
        long endTime = System.currentTimeMillis();
        logStatus.info("ELAPSED TIME: "+Utils.formatElapsedTime(startTime, endTime));
    }

    void deleteStaleAnnotations(Date pipelineStartTime) throws Exception {
        int annotsDeleted = dao.deleteAnnotations(this.getCreatedBy(), pipelineStartTime, logStatus, getStaleAnnotDeleteThreshold());
        logStatus.info("DELETED OLD ANNOTATIONS "+annotsDeleted);
    }

    void dumpStats(PipelineManager manager, int speciesTypeKey) {

        PipelineSession session = manager.getSession();
        String speciesType = SpeciesType.getCommonName(speciesTypeKey);

        logStatus.info(session.getCounterValue("highLevelGoTerm")+" " + speciesType + " lines with high level GO terms skipped");

        int counter = session.getCounterValue("IPIAnnotToCatalyticActivityTerm");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " lines with IPI annotations to catalytic activity skipped");

        logStatus.info(session.getCounterValue("unmatchedCounter")+" " + speciesType + " IDs didn't match to a gene in RGD");
        logStatus.info(session.getCounterValue("noRatGeneCounter")+" " + speciesType + " IDs without rat ortholog in RGD");
        logStatus.info(session.getCounterValue("inactiveCounter") + " inactive RGDID with " + speciesType + " ID");
        logStatus.info(session.getCounterValue("matchingAnnotCount")+" " + speciesType + " annotations match RGD");

        counter = session.getCounterValue("insertedAnnotCount");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " new annotations inserted");

        counter = session.getCounterValue("notFoundInRgdGoTermCount");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " incoming GO terms not found in RGD database");

        counter = session.getCounterValue("skippedSelfRefAnnots");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " skipped self-referencing annotations");

        counter = session.getCounterValue("DATA_SRC substitutions");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " DATA_SRC substitutions");

        // dump all counters to exception log
        manager.dumpCounters(logException);
    }

    void dumpCountsForRefRgdIds() throws Exception {
        // show current counts
        int count_0 = dao.getCountOfAnnotationForRefRgdId(getMgiRefRgdId());
        logStatus.info("COUNT REF_RGD_ID:"+getMgiRefRgdId()+" "+count_0+"  -- MGI");

        int count_1 = dao.getCountOfAnnotationForRefRgdId(getGoaHumanRefRgdId());
        logStatus.info("COUNT REF_RGD_ID:" + getGoaHumanRefRgdId() + " " + count_1+"  -- HUMAN");

        int count_2 = dao.getCountOfAnnotationForRefRgdId(getGoaDogRefRgdId());
        logStatus.info("COUNT REF_RGD_ID:" + getGoaDogRefRgdId() + " " + count_2+"  -- DOG");

        int count_ISO = dao.getCountOfAnnotationForRefRgdId(getIssRefRgdId());
        logStatus.info("COUNT REF_RGD_ID:" + getIssRefRgdId() + " " + count_ISO+"  -- ISO");
    }

    public void downloadAndProcessFiles(List<String> databases, int refRgdId, List<String> fileNames, int speciesTypeKey) throws Exception {
        List<String> localFiles = new ArrayList<>();
        for( String fileName: fileNames ) {
            localFiles.add(downloadFile(fileName));
        }
        processFile(localFiles, databases, refRgdId, speciesTypeKey);
    }

    String downloadFile(String file) throws Exception {
        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(getRemoteDir()+file);
        downloader.setLocalFile(getLocalDir()+file);
        downloader.setPrependDateStamp(true);
        return downloader.downloadNew();
    }

    public void setGoaHumanRefRgdId(int goaHumanRefRgdId) {
        this.goaHumanRefRgdId = goaHumanRefRgdId;
    }

    public int getGoaHumanRefRgdId() {
        return goaHumanRefRgdId;
    }

    public void setMgiRefRgdId(int mgiRefRgdId) {
        this.mgiRefRgdId = mgiRefRgdId;
    }

    public int getMgiRefRgdId() {
        return mgiRefRgdId;
    }

    public void setIssRefRgdId(int issRefRgdId) {
        this.issRefRgdId = issRefRgdId;
    }

    public int getIssRefRgdId() {
        return issRefRgdId;
    }

    public void setCreatedBy(int createdBy) {
        this.createdBy = createdBy;
    }

    public int getCreatedBy() {
        return createdBy;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setRemoteDir(String remoteDir) {
        this.remoteDir = remoteDir;
    }

    public String getRemoteDir() {
        return remoteDir;
    }

    public void setLocalDir(String localDir) {
        this.localDir = localDir;
    }

    public String getLocalDir() {
        return localDir;
    }

    public void setGoaHumanFiles(List<String> goaHumanFiles) {
        this.goaHumanFiles = goaHumanFiles;
    }

    public List<String> getGoaHumanFiles() {
        return goaHumanFiles;
    }

    public void setMgiFiles(List<String> mgiFiles) {
        this.mgiFiles = mgiFiles;
    }

    public List<String> getMgiFiles() {
        return mgiFiles;
    }

    public void setPipelineQueueSize(int pipelineQueueSize) {
        this.pipelineQueueSize = pipelineQueueSize;
    }

    public int getPipelineQueueSize() {
        return pipelineQueueSize;
    }

    public void setQcThreadCount(int qcThreadCount) {
        this.qcThreadCount = qcThreadCount;
    }

    public int getQcThreadCount() {
        return qcThreadCount;
    }

    public void setQc(MAHQC qc) {
        this.qc = qc;
    }

    public MAHQC getQc() {
        return qc;
    }

    public void setStaleAnnotDeleteThreshold(String staleAnnotDeleteThreshold) {
        this.staleAnnotDeleteThreshold = staleAnnotDeleteThreshold;
    }

    public String getStaleAnnotDeleteThreshold() {
        return staleAnnotDeleteThreshold;
    }

    public void setGoaHumanDbSources(List<String> goaHumanDbSources) {
        this.goaHumanDbSources = goaHumanDbSources;
    }

    public List<String> getGoaHumanDbSources() {
        return goaHumanDbSources;
    }

    public void setGoaMouseDbSources(List<String> goaMouseDbSources) {
        this.goaMouseDbSources = goaMouseDbSources;
    }

    public List<String> getGoaMouseDbSources() {
        return goaMouseDbSources;
    }

    public void setGoaDogFiles(List goaDogFiles) {
        this.goaDogFiles = goaDogFiles;
    }

    public List getGoaDogFiles() {
        return goaDogFiles;
    }

    public void setGoaDogDbSources(List goaDogDbSources) {
        this.goaDogDbSources = goaDogDbSources;
    }

    public List getGoaDogDbSources() {
        return goaDogDbSources;
    }

    public void setGoaDogRefRgdId(int goaDogRefRgdId) {
        this.goaDogRefRgdId = goaDogRefRgdId;
    }

    public int getGoaDogRefRgdId() {
        return goaDogRefRgdId;
    }

    public void setParser(MAHParser parser) {
        this.parser = parser;
    }

    public MAHParser getParser() {
        return parser;
    }
}
