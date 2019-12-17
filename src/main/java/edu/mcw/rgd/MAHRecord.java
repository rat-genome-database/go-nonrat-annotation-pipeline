package edu.mcw.rgd;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mtutaj
 * represents a line from source file, to be processed through various stages of the pipeline
 */
public class MAHRecord {

    String[] fileLine;
    String dbName;
    String dbObjectID;
    List<MAHAnnotData> annotData = new ArrayList<MAHAnnotData>();
}
