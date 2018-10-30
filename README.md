# go-nonrat-annotation-pipeline
Goals:

- Load / update GO annotations for mouse, human and dog genes from GO consortium.
- Generate inferred ISO annotations for RAT from loaded mouse, human and dog GO annotations.
- Generate inferred ISO annotations for RAT from manual chinchilla GO annotations.
- Evidence codes to make inferred rat annotations: 'EXP', 'IDA', 'IEP', 'IGI', 'IMP', 'IPI'.

The source files are loaded from ftp://ftp.geneontology.org/pub/go/gene-associations/

Logic notes:
1) for given species, multiple database sources could be processed (previously only 1 source was allowed)
2) duplicate annotations are merged by combining their XREF_SOURCE and NOTES fields
3) NOTES field is set only to information that does not appear in XREF_SOURCE field
4) implemented 5% delete threshold for stale annotations

