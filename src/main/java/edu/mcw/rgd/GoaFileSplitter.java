package edu.mcw.rgd;

/// GOA input file is huge: it contains GO annotations for all species in EBI

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GoaFileSplitter {

    ///
    public static void run(String inputFile, String outputFile) throws IOException {

        System.out.println( "starting GOA FILE SPLITTER");

        BufferedReader in = Utils.openReader(inputFile);
        BufferedWriter out = Utils.openWriter(outputFile);

        Set<String> allowedTaxonIds = new HashSet<>();
        for( int speciesTypeKey: SpeciesType.getSpeciesTypeKeys() ) {
            if( SpeciesType.isSearchable(speciesTypeKey) ) {
                int taxonId = SpeciesType.getTaxonomicId(speciesTypeKey);
                allowedTaxonIds.add(taxonId + "");
            }
        }


        long linesRead = 0;
        long linesWritten = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            linesRead++;

            if( line.startsWith("!") || line.startsWith("#") ) {
                out.write(line);
                out.write("\n");
                linesWritten++;
                continue;
            }


            // extract taxon
            int taxonPos = line.indexOf("\ttaxon:");
            if (taxonPos < 0) {
                continue;
            }
            taxonPos += 7;

            int tabPos = line.indexOf("\t", taxonPos);
            if (tabPos < 0) {
                continue;
            }
            String taxon = line.substring(taxonPos, tabPos);

            if( allowedTaxonIds.contains(taxon) ) {
                out.write(line);
                out.write("\n");
                linesWritten++;
            }
        }

        in.close();
        out.close();
        System.out.println("=== LINES READ from "+inputFile+":  "+Utils.formatThousands(linesRead));
        System.out.println("=== LINES WRITTEN to "+outputFile+":  "+Utils.formatThousands(linesWritten));
    }
}