package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public class GoNonratAnnotationPipeline {

    private String localDir;
    private List<String> mgiFiles;
    private List<String> goaMouseDbSources;
    private int mgiRefRgdId;
    private int goaAllSpeciesRefRgdId;
    private int isoRefRgdId;
    private int createdBy;
    private String version;
    private String goaAllSpeciesFile;
    private String goaAllSpeciesInRgdFile;
    private List<String> goaAllSpeciesFileSources;

    DAO dao = new DAO();
    Map<Integer,String> mapRgdIdStatus;
    Date staleAnnotCutoffDate;

    protected final Logger logStatus = LogManager.getLogger("status");
    private MAHQC qc;
    private String staleAnnotDeleteThreshold;
    private MAHParser parser;

    public static void main(String args[]) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        GoNonratAnnotationPipeline loader = (GoNonratAnnotationPipeline) (bf.getBean("loader"));
        loader.logStatus.info("--- "+loader.getVersion()+" ---");

        int argc = 0;
        for( String arg: args ) {
            if( arg.equals("--goaFileSplitter") ) {

                String goaInputFile = args[++argc];
                String goaOutputFile = args[++argc];

                GoaFileSplitter.run(goaInputFile, goaOutputFile);
                return;
            }
            argc++;
        }

        try {
            loader.run();
        } catch(Exception e) {
            Utils.printStackTrace(e, loader.logStatus);
            throw e;
        }
    }

    static Date addMinutesToDate(Date date, int minutesToAdd) {
        Date syncDate = date == null ? new Date() : date;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(syncDate);
        calendar.add(Calendar.MINUTE, minutesToAdd);
        return calendar.getTime();
    }

    public void run() throws Exception{

        long startTime = System.currentTimeMillis();
        staleAnnotCutoffDate = addMinutesToDate(new Date(), -10);

        logStatus.info("   "+dao.getConnectionInfo());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logStatus.info("   stale annots cutoff timestamp "+sdt.format(staleAnnotCutoffDate));
        logStatus.info("   started at "+sdt.format(new Date(startTime)));

        mapRgdIdStatus = dao.getStatusForGeneRgdIds();

        Map<String,Integer> refCounts = getCountsForRefRgdIds();

        downloadAllSpeciesFile();

        // process the data
        downloadAndProcessFiles(getGoaMouseDbSources(), getMgiRefRgdId(), getMgiFiles(), SpeciesType.MOUSE);

        // Note: chinchilla processing must run as the last species!
        downloadAndProcessFiles(null, getIsoRefRgdId(), null, SpeciesType.CHINCHILLA);

        logStatus.info("evidence codes to make inferred rat annotations: "
                +Utils.concatenate(qc.getEvidenceCodesToMakeRatAnnots(), ", ", "\'"));

        // evidence codes without counts
        logStatus.info("evidence codes not used to make rat annotations: "
                +Utils.concatenate(MAHQC.wrongEvidenceCounts.keySet(), ", ", "\'"));

        int originalIsoAnnotCount = refCounts.get(getIsoRefRgdId()+"|"+0);
        deleteObsoleteIsoAnnotationsForRat(originalIsoAnnotCount);

        // show current counts
        dumpCountsForRefRgdIds(refCounts);

        // show total elapsed time
        long endTime = System.currentTimeMillis();
        logStatus.info("OK   -- ELAPSED TIME: "+Utils.formatElapsedTime(startTime, endTime));
    }

    public void processFile(List<String> fileNames, List<String> fromDatabases, int internalRefRGDID, int speciesTypeKey) throws Exception{

        parser.init(fileNames, fromDatabases, speciesTypeKey, dao);
        qc.init(dao, mapRgdIdStatus, internalRefRGDID, createdBy, isoRefRgdId, speciesTypeKey);
        MAHDL dl = new MAHDL(dao);

        CounterPool counters = new CounterPool();

        logStatus.info("START PARSER for  "+SpeciesType.getShortName(speciesTypeKey));
        List<MAHRecord> records = parser.process(logStatus);

        // randomize incoming records to minimize risk of conflicts
        Collections.shuffle(records);

        logStatus.info("   qc starts for "+Utils.formatThousands(records.size())+" incoming lines");
        records.parallelStream().forEach( rec -> {

            try {
                qc.process(rec, counters);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });

        logStatus.debug("   WITH_INFO consolidation starts...");
        WithInfoConsolidator.run(records, counters);

        logStatus.info("   dl starts for "+Utils.formatThousands(records.size())+" incoming lines");
        records.parallelStream().forEach( rec -> {

            try {
                dl.process(rec, counters);
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

        counter = counters.get("updatedOrigCreatedDate");
        if( counter!=0 )
            logStatus.info("    "+counter+" " + speciesType + " annotations with ORIGINAL_CREATED_DATE updated");

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

        counter = counters.get("with_info merges");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " incoming lines merged by WITH_INFO");

        counter = counters.get("incoming lines removed");
        if( counter!=0 )
            logStatus.info(counter+" " + speciesType + " incoming lines removed due to WITH_INFO consolidation WITH_INFO");

        // dump all counters to detail log
        logStatus.debug(counters.dumpAlphabetically());
    }

    Map<String,Integer> getCountsForRefRgdIds() throws Exception {

        Map<String,Integer> counts = new HashMap<>();

        // show current counts
        counts.put(getMgiRefRgdId()+"|"+SpeciesType.MOUSE, dao.getCountOfAnnotationForRefRgdId(getMgiRefRgdId()));
        counts.put(getIsoRefRgdId()+"|"+0, dao.getCountOfAnnotationForRefRgdId(getIsoRefRgdId()));

        for( int sp: SpeciesType.getSpeciesTypeKeys() ) {
            if( sp==SpeciesType.MOUSE || sp==SpeciesType.RAT || !SpeciesType.isSearchable(sp) ) {
                continue;
            }
            counts.put(getGoaAllSpeciesRefRgdId()+"|"+sp, dao.getCountOfAnnotationsByReferenceAndSpecies(getGoaAllSpeciesRefRgdId(), sp));
        }

        return counts;
    }

    void dumpCountsForRefRgdIds(Map<String,Integer> counts) throws Exception {

        for( Map.Entry<String,Integer> entry: counts.entrySet() ) {
            String[] cols = entry.getKey().split("[\\|]");
            int refRgdId = Integer.parseInt(cols[0]);
            int speciesTypeKey = Integer.parseInt(cols[1]);

            String species;
            if( speciesTypeKey==0 ) {
                species = "RAT ISO";
            } else {
                species = SpeciesType.getCommonName(speciesTypeKey).toUpperCase();
            }

            int newCount;
            if( speciesTypeKey!=0 ) {
                newCount = dao.getCountOfAnnotationsByReferenceAndSpecies(refRgdId, speciesTypeKey);
            } else {
                newCount = dao.getCountOfAnnotationForRefRgdId(refRgdId);
            }
            dumpCount(entry.getValue(), newCount, "COUNT REF_RGD_ID:"+refRgdId+"  ["+species+"]  ");
        }
    }

    void dumpCount(int oldCount, int newCount, String info) {
        int diffCount = newCount - oldCount;
        String diffCountStr = diffCount!=0 ? "     difference: "+ _plusMinusNF.format(diffCount) : "     no changes";
        logStatus.info(info+": "+Utils.formatThousands(newCount)+diffCountStr);
    }
    private NumberFormat _plusMinusNF = new DecimalFormat(" +###,###,###; -###,###,###");

    public void downloadAndProcessFiles(List<String> databases, int refRgdId, List<String> fileNames, int speciesTypeKey) throws Exception {

        long time0 = System.currentTimeMillis();

        int count0 = dao.getCountOfAnnotationsByReferenceAndSpecies(refRgdId, speciesTypeKey);

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
        int annotsDeleted = dao.deleteAnnotations(getCreatedBy(), staleAnnotCutoffDate, logStatus, getStaleAnnotDeleteThreshold(), refRgdId, count0, speciesTypeKey);
        if( annotsDeleted!=0 ) {
            logStatus.info(annotsDeleted + " " + speciesName + " STALE ANNOTATIONS DELETED");
        }

        logStatus.info("===  "+speciesName+" OK  === elapsed "+Utils.formatElapsedTime(time0, System.currentTimeMillis()));
        logStatus.info("");
    }


    void deleteObsoleteIsoAnnotationsForRat( int originalIsoAnnotCount ) throws Exception {

        int speciesTypeKey = SpeciesType.RAT;
        String speciesName = SpeciesType.getCommonName(speciesTypeKey);

        // delete annotations not updated/inserted by the pipeline
        int annotsDeleted = dao.deleteAnnotations(getCreatedBy(), staleAnnotCutoffDate, logStatus, getStaleAnnotDeleteThreshold(),
                getIsoRefRgdId(), originalIsoAnnotCount, speciesTypeKey);
        if( annotsDeleted!=0 ) {
            logStatus.info(annotsDeleted + " " + speciesName + " STALE ANNOTATIONS DELETED");
        }
    }

    String downloadFile(String file) throws Exception {

        if( file.startsWith("ftp") || file.startsWith("http") ) {
            // determine local file name
            int lastSlashPos = file.lastIndexOf('/');
            String localFileName = getLocalDir() + file.substring(lastSlashPos);

            FileDownloader downloader = new FileDownloader();
            downloader.setExternalFile(file);
            downloader.setLocalFile(localFileName);
            downloader.setPrependDateStamp(true);
            return downloader.downloadNew();
        } else {
            return file;
        }
    }

    /**
     * download huge file with all species; then create another much smaller file with public species in rgd
     * except mouse (file from MGI) and RAT (different pipeline)
     */
    void downloadAllSpeciesFile() throws Exception {

        AllSpeciesFileSplitter fileSplitter = new AllSpeciesFileSplitter();
        String lastAllSpeciesFile = fileSplitter.downloadIfNew(getGoaAllSpeciesFile());
        String lastRgdSpeciesFile = fileSplitter.filterRgdSpecies(lastAllSpeciesFile);

        Map<Integer, String> fileMap = fileSplitter.extractFilesForRgdSpecies(lastRgdSpeciesFile);

        for( Map.Entry<Integer, String> entry: fileMap.entrySet() ) {
            int speciesTypeKey = entry.getKey();
            String fileName = entry.getValue();

            List<String> filesProcessed = new ArrayList<>();
            filesProcessed.add(fileName);

            downloadAndProcessFiles(getGoaAllSpeciesFileSources(), getGoaAllSpeciesRefRgdId(), filesProcessed, speciesTypeKey);
        }
    }

    public void setMgiRefRgdId(int mgiRefRgdId) {
        this.mgiRefRgdId = mgiRefRgdId;
    }

    public int getMgiRefRgdId() {
        return mgiRefRgdId;
    }

    public void setIsoRefRgdId(int issRefRgdId) {
        this.isoRefRgdId = issRefRgdId;
    }

    public int getIsoRefRgdId() {
        return isoRefRgdId;
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

    public void setGoaMouseDbSources(List<String> goaMouseDbSources) {
        this.goaMouseDbSources = goaMouseDbSources;
    }

    public List<String> getGoaMouseDbSources() {
        return goaMouseDbSources;
    }

    public void setParser(MAHParser parser) {
        this.parser = parser;
    }

    public MAHParser getParser() {
        return parser;
    }

    public int getGoaAllSpeciesRefRgdId() {
        return goaAllSpeciesRefRgdId;
    }

    public void setGoaAllSpeciesRefRgdId(int goaAllSpeciesRefRgdId) {
        this.goaAllSpeciesRefRgdId = goaAllSpeciesRefRgdId;
    }

    public String getGoaAllSpeciesFile() {
        return goaAllSpeciesFile;
    }

    public void setGoaAllSpeciesFile(String goaAllSpeciesFile) {
        this.goaAllSpeciesFile = goaAllSpeciesFile;
    }

    public String getGoaAllSpeciesInRgdFile() {
        return goaAllSpeciesInRgdFile;
    }

    public void setGoaAllSpeciesInRgdFile(String goaAllSpeciesInRgdFile) {
        this.goaAllSpeciesInRgdFile = goaAllSpeciesInRgdFile;
    }

    public List<String> getGoaAllSpeciesFileSources() {
        return goaAllSpeciesFileSources;
    }

    public void setGoaAllSpeciesFileSources(List<String> goaAllSpeciesFileSources) {
        this.goaAllSpeciesFileSources = goaAllSpeciesFileSources;
    }
}
