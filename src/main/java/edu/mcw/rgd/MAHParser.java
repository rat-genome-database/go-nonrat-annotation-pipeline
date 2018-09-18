package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.pipelines.RecordPreprocessor;

import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Created by mtutaj on 3/2/2018.
 */
public class MAHParser extends RecordPreprocessor {

    private int speciesTypeKey = 0;
    private List<String> fileNames = null;
    private List<String> fromDatabases = null;
    private MouseAndHumanGoAnnotationDAO dao;

    public void init( List<String> fileNames, List<String> fromDatabases, int speciesTypeKey, MouseAndHumanGoAnnotationDAO dao ) {

        this.fileNames = fileNames;
        this.fromDatabases = fromDatabases;
        this.speciesTypeKey = speciesTypeKey;
        this.dao = dao;
    }

    @Override
    public void process() throws Exception {
        if( speciesTypeKey== SpeciesType.CHINCHILLA ) {
            processForChinchilla();
            return;
        }

        int recNo = 0;
        for( String fileName: fileNames ) {
            InputStream fis = new FileInputStream(fileName);
            GZIPInputStream gis = new GZIPInputStream(fis);
            Scanner s = new Scanner(gis);
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] lineCols = line.split("(\\t)");
                String dbName = lineCols[0];
                // skip lines that do not begin with db name (f.e. comment lines start with '!')
                if (!fromDatabases.contains(dbName)) {
                    continue;
                }

                MAHRecord rec = new MAHRecord();
                rec.setRecNo(++recNo);
                rec.fileLine = lineCols;
                rec.dbName = dbName;
                getSession().putRecordToFirstQueue(rec);
            }
            fis.close();
        }
    }

    void processForChinchilla() throws Exception {

        String taxon = "taxon:"+SpeciesType.getTaxonomicId(speciesTypeKey);
        SimpleDateFormat sdt = new SimpleDateFormat("yyyyMMdd");

        int recNo = 0;
        List<Annotation> goAnnots = dao.getManualGoAnnotsForChinchilla();
        for( Annotation annot: goAnnots ) {
            // turn an annot into a GAF line
            String[] lineCols = new String[15];
            lineCols[0] = annot.getDataSrc(); // DB
            lineCols[1] = annot.getAnnotatedObjectRgdId().toString(); // DB Object ID
            lineCols[2] = annot.getObjectSymbol(); // DB Object Symbol
            lineCols[3] = annot.getQualifier(); // Qualifier
            lineCols[4] = annot.getTermAcc(); // GO ID
            lineCols[5] = "RGD:"+annot.getRefRgdId();
            if( annot.getXrefSource()!=null ) { // DB Ref
                lineCols[5] += "|"+annot.getXrefSource();
            }
            lineCols[6] = annot.getEvidence(); // Evidence Code
            lineCols[7] = annot.getWithInfo(); // With or From
            lineCols[8] = annot.getAspect(); // Aspect
            lineCols[9] = annot.getObjectName(); // DB Object Name
            lineCols[10] = null; // DB Object Synonyms
            lineCols[11] = "gene"; // DB Object Type
            lineCols[12] = taxon;
            lineCols[13] = sdt.format(annot.getCreatedDate());
            lineCols[14] = "RGD"; // Assigned By

            MAHRecord rec = new MAHRecord();
            rec.setRecNo(++recNo);
            rec.fileLine = lineCols;
            rec.dbName = "RGD";
            getSession().putRecordToFirstQueue(rec);
        }
    }
}
