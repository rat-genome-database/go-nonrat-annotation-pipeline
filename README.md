# go-nonrat-annotation-pipeline
Goals:

- Load / update GO annotations for all species (except RAT) in RGD from GO consortium.
- Generate inferred ISO annotations for RAT from loaded GO annotations.
- Generate inferred ISO annotations for RAT from manual chinchilla GO annotations.
- Evidence codes to make inferred rat annotations: 'EXP', 'IDA', 'IEP', 'IGI', 'IMP', 'IPI'.

Source for mouse GO annotations:
http://www.informatics.jax.org/downloads/reports/gene_association.mgi.gz

Source for GO annotations for species other than mouse and rat:
ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/UNIPROT/goa_uniprot_all.gaf.gz

Logic notes:
1) for given species, multiple database sources could be processed (previously only 1 source was allowed)
2) duplicate annotations are merged by combining their XREF_SOURCE and NOTES fields
3) NOTES field is set only to information that does not appear in XREF_SOURCE field
4) if XREF_SOURCE field contains multiple PMIDs, NOTES field allows to reconstruct the original associations between database and PMID
5) implemented 5% delete threshold for stale annotations
6) MGI:MGI:xxxxxxx is replaced with MGI:xxxxxxxx

