package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/**
 * - download and process mouse, human, dog and pig GO annotations from GO consortium
 * - generates inferred rat annotations from GO manual chinchilla annotations
 * <p>
 * Logic for mouse-human-dog-pig: <ul>
 * <li>created-date of incoming annotations that are inserted into db is set to SYSDATE
 * <li>last-modified-date is set to SYSDATE every time the annotation is processed through the pipeline
 * <li>created-date is refreshed (set to SYSDATE) for IEA annotations with created-date older than 9 months
 * <li>NOTES is set to ids that are different from the one in XREF_SOURCE
 * <li>NOTES and XREF_SOURCE fields are combined for similar annotations
 * </ul>
 */
public class MouseAndHumanGoAnnotationPipeline {

    private String localDir;
    private List<String> mgiFiles;
    private List<String> goaHumanFiles;
    private List<String> goaHumanDbSources;
    private List<String> goaMouseDbSources;
    private List<String> goaDogFiles;
    private List<String> goaDogDbSources;
    private List<String> goaPigFiles;
    private List<String> goaPigDbSources;
    private int goaHumanRefRgdId;
    private int mgiRefRgdId;
    private int goaDogRefRgdId;
    private int goaPigRefRgdId;
    private int issRefRgdId;
    private int createdBy;
    private String version;

    DAO dao = new DAO();
    Map<Integer,String> mapRgdIdStatus;
    Date staleAnnotCutoffDate;

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

        try {
            loader.run();
        } catch(Exception e) {
            Utils.printStackTrace(e, loader.logStatus);
            throw e;
        }
    }

    public void run() throws Exception{

        long startTime = System.currentTimeMillis();
        staleAnnotCutoffDate = Utils.addHoursToDate(new Date(), -1);

        logStatus.info("   "+dao.getConnectionInfo());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logStatus.info("   started at "+sdt.format(new Date(startTime)));

        mapRgdIdStatus = dao.getStatusForGeneRgdIds();

        // show current counts
        Map<Integer,Integer> refCounts = getCountsForRefRgdIds();

        // process the data
        downloadAndProcessFiles(getGoaHumanDbSources(), getGoaHumanRefRgdId(), getGoaHumanFiles(), SpeciesType.HUMAN);
        downloadAndProcessFiles(getGoaMouseDbSources(), getMgiRefRgdId(), getMgiFiles(), SpeciesType.MOUSE);
        downloadAndProcessFiles(getGoaDogDbSources(), getGoaDogRefRgdId(), getGoaDogFiles(), SpeciesType.DOG);
        downloadAndProcessFiles(getGoaPigDbSources(), getGoaPigRefRgdId(), getGoaPigFiles(), SpeciesType.PIG);

        // Note: chinchilla processing must run as the last species!
        downloadAndProcessFiles(null, getIssRefRgdId(), null, SpeciesType.CHINCHILLA);

        logStatus.info("evidence codes to make inferred rat annotations: "
                +Utils.concatenate(qc.getEvidenceCodesToMakeRatAnnots(), ", ", "\'"));

        if( false ) {
            // evidence codes with counts
            logStatus.info("evidence codes not used to make rat annotations:");
            for (Map.Entry<String, Integer> entry : MAHQC.wrongEvidenceCounts.entrySet()) {
                logStatus.info("  " + entry.getKey() + ": " + entry.getValue());
            }
        } else {
            // evidence codes without counts
            logStatus.info("evidence codes not used to make rat annotations: "
                    +Utils.concatenate(MAHQC.wrongEvidenceCounts.keySet(), ", ", "\'"));
        }

        // show current counts
        dumpCountsForRefRgdIds(refCounts);

        // show total elapsed time
        long endTime = System.currentTimeMillis();
        logStatus.info("OK   -- ELAPSED TIME: "+Utils.formatElapsedTime(startTime, endTime));
    }

    public void processFile(List<String> fileNames, List<String> fromDatabases, int internalRefRGDID, int speciesTypeKey) throws Exception{

        parser.init(fileNames, fromDatabases, speciesTypeKey, dao);
        qc.init(dao, mapRgdIdStatus, internalRefRGDID, createdBy, issRefRgdId, speciesTypeKey);
        MAHDL dl = new MAHDL(dao);

        CounterPool counters = new CounterPool();

        List<MAHRecord> records = parser.process();

        // randomize incoming records to minimize risk of conflicts
        Collections.shuffle(records);

        records.parallelStream().forEach( rec -> {

            try {
                qc.process(rec, counters);
                dl.process(rec);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
        dl.postProcess(counters);

        dumpStats(counters, speciesTypeKey);
    }

    void dumpStats(CounterPool counters, int speciesTypeKey) {

        String speciesType = SpeciesType.getCommonName(speciesTypeKey);

        logStatus.info(counters.get("highLevelGoTerm")+" " + speciesType + " lines with high level GO terms skipped");

        int counter = counters.get("IPIAnnotToCatalyticActivityTerm");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " lines with IPI annotations to catalytic activity skipped");

        logStatus.info(counters.get("unmatchedCounter")+" " + speciesType + " IDs didn't match to a gene in RGD");
        logStatus.info(counters.get("noRatGeneCounter")+" " + speciesType + " IDs without rat ortholog in RGD");
        logStatus.info(counters.get("inactiveCounter") + " inactive RGDID with " + speciesType + " ID");
        logStatus.info(counters.get("matchingAnnotCount")+" " + speciesType + " annotations match RGD -- updated LAST_MODIFIED_DATE");

        counter = counters.get("insertedAnnotCount");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " new annotations inserted");

        counter = counters.get("updatedAnnotCount");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " annotations updated");

        counter = counters.get("notFoundInRgdGoTermCount");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " incoming GO terms not found in RGD database");

        counter = counters.get("skippedSelfRefAnnots");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " skipped self-referencing annotations");

        counter = counters.get("skippedIsoAnnots");
        if( counter!=0 ) {
            logStatus.info(counter + " " + speciesType + " skipped ISO annotations with empty WITH_INFO field");
        }

        counter = counters.get("DATA_SRC substitutions");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " DATA_SRC substitutions");

        // dump all counters to exception log
        logException.info(counters.dumpAlphabetically());
    }

    Map<Integer,Integer> getCountsForRefRgdIds() throws Exception {

        Map<Integer,Integer> counts = new HashMap<>();

        // show current counts
        counts.put(getMgiRefRgdId(), dao.getCountOfAnnotationForRefRgdId(getMgiRefRgdId()));
        counts.put(getGoaHumanRefRgdId(), dao.getCountOfAnnotationForRefRgdId(getGoaHumanRefRgdId()));
        counts.put(getGoaDogRefRgdId(), dao.getCountOfAnnotationForRefRgdId(getGoaDogRefRgdId()));
        counts.put(getGoaPigRefRgdId(), dao.getCountOfAnnotationForRefRgdId(getGoaPigRefRgdId()));
        counts.put(getIssRefRgdId(), dao.getCountOfAnnotationForRefRgdId(getIssRefRgdId()));

        return counts;
    }

    void dumpCountsForRefRgdIds(Map<Integer,Integer> counts) throws Exception {
        // show current counts
        int newCount = dao.getCountOfAnnotationForRefRgdId(getMgiRefRgdId());
        dumpCount(counts.get(getMgiRefRgdId()), newCount, "COUNT REF_RGD_ID:"+getMgiRefRgdId()+"  [MGI]  ");

        newCount = dao.getCountOfAnnotationForRefRgdId(getGoaHumanRefRgdId());
        dumpCount(counts.get(getGoaHumanRefRgdId()), newCount, "COUNT REF_RGD_ID:" + getGoaHumanRefRgdId() + "  [HUMAN]  ");

        newCount = dao.getCountOfAnnotationForRefRgdId(getGoaDogRefRgdId());
        dumpCount(counts.get(getGoaDogRefRgdId()), newCount,"COUNT REF_RGD_ID:" + getGoaDogRefRgdId() + "  [DOG]  ");

        newCount = dao.getCountOfAnnotationForRefRgdId(getGoaPigRefRgdId());
        dumpCount(counts.get(getGoaPigRefRgdId()), newCount,"COUNT REF_RGD_ID:" + getGoaPigRefRgdId() + "  [PIG]  ");

        newCount = dao.getCountOfAnnotationForRefRgdId(getIssRefRgdId());
        dumpCount(counts.get(getIssRefRgdId()), newCount,"COUNT REF_RGD_ID:" + getIssRefRgdId() + "  [RAT ISO]  ");
    }

    void dumpCount(int oldCount, int newCount, String info) {
        int diffCount = newCount - oldCount;
        String diffCountStr = diffCount!=0 ? "     difference: "+ _plusMinusNF.format(diffCount) : "     no changes";
        logStatus.info(info+": "+Utils.formatThousands(newCount)+diffCountStr);
    }
    private NumberFormat _plusMinusNF = new DecimalFormat(" +###,###,###; -###,###,###");

    public void downloadAndProcessFiles(List<String> databases, int refRgdId, List<String> fileNames, int speciesTypeKey) throws Exception {

        long time0 = System.currentTimeMillis();

        int count0 = dao.getCountOfAnnotationForRefRgdId(refRgdId);

        if( fileNames==null ) {
            // chinchilla does not have any external source files
            processFile(null, null, 0, speciesTypeKey);
        } else {
            List<String> localFiles = new ArrayList<>();
            for (String fileName : fileNames) {
                localFiles.add(downloadFile(fileName));
            }
            processFile(localFiles, databases, refRgdId, speciesTypeKey);
        }
        String speciesName = SpeciesType.getCommonName(speciesTypeKey);

        // delete annotations not updated/inserted by the pipeline
        int annotsDeleted = dao.deleteAnnotations(getCreatedBy(), staleAnnotCutoffDate, logStatus, getStaleAnnotDeleteThreshold(), refRgdId, count0);
        if( annotsDeleted!=0 ) {
            logStatus.info(annotsDeleted + " " + speciesName + " STALE ANNOTATIONS DELETED");
        }

        logStatus.info("===  "+speciesName+" OK  === elapsed "+Utils.formatElapsedTime(time0, System.currentTimeMillis()));
        logStatus.info("");
    }

    String downloadFile(String file) throws Exception {
        // determine local file name
        int lastSlashPos = file.lastIndexOf('/');
        String localFileName = getLocalDir() + file.substring(lastSlashPos);

        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(file);
        downloader.setLocalFile(localFileName);
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

    public List<String> getGoaPigFiles() {
        return goaPigFiles;
    }

    public void setGoaPigFiles(List<String> goaPigFiles) {
        this.goaPigFiles = goaPigFiles;
    }

    public List<String> getGoaPigDbSources() {
        return goaPigDbSources;
    }

    public void setGoaPigDbSources(List<String> goaPigDbSources) {
        this.goaPigDbSources = goaPigDbSources;
    }

    public int getGoaPigRefRgdId() {
        return goaPigRefRgdId;
    }

    public void setGoaPigRefRgdId(int goaPigRefRgdId) {
        this.goaPigRefRgdId = goaPigRefRgdId;
    }
}
