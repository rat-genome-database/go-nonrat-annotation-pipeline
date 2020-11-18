package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mtutaj on 3/2/2018.
 */
public class MAHParser {

    private int speciesTypeKey = 0;
    private List<String> fileNames = null;
    private List<String> fromDatabases = null;
    private DAO dao;

    public void init( List<String> fileNames, List<String> fromDatabases, int speciesTypeKey, DAO dao ) {

        this.fileNames = fileNames;
        this.fromDatabases = fromDatabases;
        this.speciesTypeKey = speciesTypeKey;
        this.dao = dao;
    }

    public List<MAHRecord> process() throws Exception {
        if( speciesTypeKey == SpeciesType.CHINCHILLA ) {
            return processForChinchilla();
        }

        List<MAHRecord> records = new ArrayList<>();
        for( String fileName: fileNames ) {
            BufferedReader in = Utils.openReader(fileName);
            String line;
            while( (line=in.readLine())!=null ) {
                // skip comment lines
                if( line.startsWith("!") ) {
                    continue;
                }

                String line2 = line.replace("MGI:MGI:", "MGI:");

                String[] lineCols = line2.split("(\\t)", -1);
                String dbName = lineCols[0];
                // skip lines that do not begin with db name (f.e. comment lines start with '!')
                if (!fromDatabases.contains(dbName)) {
                    continue;
                }

                MAHRecord rec = new MAHRecord();
                rec.fileLine = lineCols;
                rec.dbName = dbName;
                records.add(rec);
            }
            in.close();
        }
        return records;
    }

    List<MAHRecord> processForChinchilla() throws Exception {

        String taxon = "taxon:"+SpeciesType.getTaxonomicId(speciesTypeKey);
        SimpleDateFormat sdt = new SimpleDateFormat("yyyyMMdd");

        List<MAHRecord> records = new ArrayList<>();
        List<Annotation> goAnnots = dao.getManualGoAnnotsForChinchilla();
        for( Annotation annot: goAnnots ) {
            // turn an annot into a GAF line
            String[] lineCols = new String[17];
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
            lineCols[15] = annot.getAnnotationExtension();
            lineCols[16] = annot.getGeneProductFormId();

            MAHRecord rec = new MAHRecord();
            rec.fileLine = lineCols;
            rec.dbName = "RGD";
            records.add(rec);
        }
        return records;
    }
}
