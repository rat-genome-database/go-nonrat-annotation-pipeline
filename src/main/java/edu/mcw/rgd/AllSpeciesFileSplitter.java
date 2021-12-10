package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 */
public class AllSpeciesFileSplitter {

    String dir = "data/allSpecies";
    String fname = "goa_uniprot_all.gaf.gz";

    public String downloadIfNew(String srcFile) throws Exception {

        // archival files ending with 'goa_uniprot_all.gaf.gz' sorted alphabetically
        List<String> arFiles = getListOfArchivalFiles();

        FileDownloader fd = new FileDownloader();
        fd.setExternalFile(srcFile);
        fd.setLocalFile(dir+"/goa_uniprot_all.gaf.gz");
        fd.setPrependDateStamp(true);
        String localFile = fd.downloadNew();
        String localFileFullPath = new File(localFile).getAbsolutePath();

        if( !arFiles.contains(localFileFullPath) ) {
            arFiles.add(localFileFullPath);
        }

        // if the latest file has the same size as previously downloaded file, delete the previous file
        if( arFiles.size()>1 ) {
            int lastFileIndex = arFiles.size()-1;
            long lastFileLen = new File(arFiles.get(lastFileIndex)).length();

            for( int i=lastFileIndex-1; i>=0; i-- ) {
                File f = new File(arFiles.get(i));
                if( f.length()==lastFileLen ) {
                    f.delete();
                }
            }
        }

        return arFiles.get(arFiles.size()-1);
    }

    List<String> getListOfArchivalFiles() {

        File fdir = new File(dir);
        if( !fdir.exists() ) {
            fdir.mkdirs();
        }

        List<String> fileList = new ArrayList<>();

        File[] files = fdir.listFiles();
        for( File f: files ) {
            if( f.isFile() && f.getName().endsWith(fname) ) {
                fileList.add(f.getAbsolutePath());
            }
        }

        Collections.sort(fileList);

        return fileList;
    }

    Map<Integer, String> extractFilesForRgdSpecies( String fileName ) throws Exception {

        DAO dao = new DAO();

        // map of species type to file name
        Map<Integer, String> resultMap = new HashMap<>();

        // map taxon id (surrounded by TABs) to BufferedReader
        Map<String, BufferedWriter> fileMap = new HashMap<>();

        int notNullCount = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dateAsString = sdf.format(new java.util.Date())+"_";

        for( int speciesTypeKey: SpeciesType.getSpeciesTypeKeys() ) {
            if( speciesTypeKey==SpeciesType.RAT || speciesTypeKey==SpeciesType.MOUSE ) {
                continue;
            }
            if( SpeciesType.isSearchable(speciesTypeKey) ) {
                int taxonId = SpeciesType.getTaxonomicId(speciesTypeKey);
                String species = dao.getSpeciesShortName(speciesTypeKey);

                String fname = dir + "/" + dateAsString + species + ".gaf.gz";
                resultMap.put(speciesTypeKey, fname);

                if( new File(fname).exists() ) {
                    fileMap.put(taxonId+"", null);
                } else {
                    BufferedWriter out = Utils.openWriter(fname);
                    fileMap.put(taxonId + "", out);
                    notNullCount++;
                }
            }
        }

        if( notNullCount>0 ) {
            BufferedReader in = Utils.openReader(fileName);
            String line;
            while ((line = in.readLine()) != null) {
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

                BufferedWriter out = fileMap.get(taxon);
                if (out != null) {
                    out.write(line);
                    out.write("\n");
                }
            }
            in.close();
        }

        for( BufferedWriter w: fileMap.values() ) {
            if( w!=null )
                w.close();
        }
        return resultMap;
    }
}
