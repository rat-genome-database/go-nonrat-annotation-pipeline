package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
    private int goaAllSpeciesRefRgdId;
    private int issRefRgdId;
    private int createdBy;
    private String version;
    private String goaAllSpeciesFile;
    private String goaAllSpeciesInRgdFile;

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
        GoNonratAnnotationPipeline loader = (GoNonratAnnotationPipeline) (bf.getBean("loader"));
        loader.logStatus.info("--- "+loader.getVersion()+" ---");

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

        logStatus.info("   parser starts...");
        List<MAHRecord> records = parser.process();

        // randomize incoming records to minimize risk of conflicts
        Collections.shuffle(records);

        logStatus.info("   qc starts for "+records.size()+" incoming lines");
        records.parallelStream().forEach( rec -> {

            try {
                qc.process(rec, counters);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });

        logStatus.info("   WITH_INFO consolidation starts...");
        WithInfoConsolidator.run(records, counters);

        logStatus.info("   dl starts for "+records.size()+" incoming lines");
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

        // dump all counters to exception log
        logException.info(counters.dumpAlphabetically());
    }

    Map<String,Integer> getCountsForRefRgdIds() throws Exception {

        Map<String,Integer> counts = new HashMap<>();

        // show current counts
        counts.put(getMgiRefRgdId()+"|"+SpeciesType.MOUSE, dao.getCountOfAnnotationForRefRgdId(getMgiRefRgdId()));
        counts.put(getGoaHumanRefRgdId()+"|"+SpeciesType.HUMAN, dao.getCountOfAnnotationForRefRgdId(getGoaHumanRefRgdId()));
        counts.put(getGoaDogRefRgdId()+"|"+SpeciesType.DOG, dao.getCountOfAnnotationForRefRgdId(getGoaDogRefRgdId()));
        counts.put(getGoaPigRefRgdId()+"|"+SpeciesType.PIG, dao.getCountOfAnnotationForRefRgdId(getGoaPigRefRgdId()));
        counts.put(getIssRefRgdId()+"|"+0, dao.getCountOfAnnotationForRefRgdId(getIssRefRgdId()));

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

        List<String> sourcesProcessed = new ArrayList<>();
        sourcesProcessed.add("UniProtKB");

        AllSpeciesFileSplitter fileSplitter = new AllSpeciesFileSplitter();
        String lastAllSpeciesFile = fileSplitter.downloadIfNew(getGoaAllSpeciesFile());
        Map<Integer, String> fileMap = fileSplitter.extractFilesForRgdSpecies(lastAllSpeciesFile);
        if( true ) {

            for( Map.Entry<Integer, String> entry: fileMap.entrySet() ) {
                int speciesTypeKey = entry.getKey();
                String fileName = entry.getValue();

                List<String> filesProcessed = new ArrayList<>();
                filesProcessed.add(fileName);

                downloadAndProcessFiles(sourcesProcessed, getGoaAllSpeciesRefRgdId(), filesProcessed, speciesTypeKey);
            }

            return;
        }



        String localFile = downloadFile(getGoaAllSpeciesFile());
        logStatus.info("Downloaded "+getGoaAllSpeciesFile());

        String msg = "   taxons to be processed: ";
        Set<String> processedTaxons = new HashSet<>();
        for( int speciesType: SpeciesType.getSpeciesTypeKeys() ) {

            if( speciesType==SpeciesType.RAT || speciesType==SpeciesType.MOUSE ) {
                continue;
            }
            if( !SpeciesType.isSearchable(speciesType) ) {
                continue;
            }
            int taxonId = SpeciesType.getTaxonomicId(speciesType);
            String taxon = Integer.toString(taxonId);
            msg += taxon +" ";
            processedTaxons.add("taxon:"+taxon);
        }

        logStatus.info(msg);

        BufferedReader in = Utils.openReader(localFile);
        String prefix = localFile.substring(0, getLocalDir().length()+10);
        BufferedWriter out = Utils.openWriter(prefix+getGoaAllSpeciesInRgdFile());
        long totalLinesProcessed = 0;
        int linesWithSpeciesInRgd = 0;
        Map<String,Integer> annotsPerSpeciesMap = new HashMap<>();

        String line;
        while( (line=in.readLine())!=null ) {
            // copy comment lines as is
            totalLinesProcessed++;
            if( line.startsWith("!") ) {
                out.write(line);
                out.write("\n");
                continue;
            }

            // taxon filter
            String taxon = extractTaxonColumn(line);
            if( taxon!=null && processedTaxons.contains(taxon) ) {
                out.write(line);
                out.write("\n");
                linesWithSpeciesInRgd++;

                Integer annotCount = annotsPerSpeciesMap.get(taxon);
                if( annotCount==null ) {
                    annotCount = 1;
                } else {
                    annotCount++;
                }
                annotsPerSpeciesMap.put(taxon, annotCount);
            }
        }
        in.close();
        out.close();

        logStatus.info("   total lines processed: "+Utils.formatThousands(totalLinesProcessed));
        logStatus.info("   lines with species in RGD: "+Utils.formatThousands(linesWithSpeciesInRgd));
        for( Map.Entry<String, Integer> entry: annotsPerSpeciesMap.entrySet() ) {
            String speciesName = SpeciesType.getCommonName(SpeciesType.parse(entry.getKey()));
            logStatus.info("      lines for "+speciesName+": " + Utils.formatThousands(entry.getValue()));
        }

        logStatus.info("=====");
    }

    String extractTaxonColumn(String line) {

        // taxon is 13th column in the GAF file
        int pos = 0;
        for( int i=0; i<12; i++ ) {
            int pos2 = line.indexOf('\t', pos);
            if( pos2<0 ) {
                return null;
            }
            pos = pos2+1;
        }

        int pos2 = line.indexOf('\t', pos);

        return line.substring(pos, pos2);
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
}