package edu.mcw.rgd;

import edu.mcw.rgd.datamodel.ontology.Annotation;

import java.util.List;

/**
 * @author mtutaj
 * Date: 1/7/15
 * <p>
 * holds information about the annotation data
 * Note: there could be multiple annotation data pieces per MAHRecord
 */
public class MAHAnnotData {
    Annotation incomingAnnot;
    String db;
    String key;
    String dbObjectId;
    List<MAHAnnotData> parentList;
}
