import logging
import os
import sys

from Configs import getConfig
from linetimer import CodeTimer

from compound_parser import load_compounds
from mesh_desc_parser import load_mesh_descriptor_files
from uberon_parser import load_uberon_anatomy_file
from pubmed_search import get_disease_pmid_file, get_anatomy_pmid_file, get_disease_anatomy_cooccurrence
from disease_ontology_parser import load_disease_file
from download_data import download
from drugbank_parser import load_drugbank_file
from load_data import load_data, load_data_mp
from ttd_target_parser import (load_target_file,
                               load_ttd_target_compound_map_file)

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )
    SCRIPT_DIR = os.path.join(SCRIPT_DIR, "..")
    sys.path.append(os.path.normpath(SCRIPT_DIR))


# Adapt to CovidGraph Dataloaders env API


if __name__ == "__main__":
    config = getConfig()
    with CodeTimer("Downloader", unit="s"):
        download()
    with CodeTimer("Importer", unit="s"):

        load_compounds(config.DRUGBANK_VOCABULARY_FILE, config.DRUGBANK_XML_FILE, config.TTD_DRUG_DOWNLOAD_FILE, config.TTD_DRUG_XREF_DOWNLOAD_FILE,
                       config.DRUGBANK_COMPOUND_FILE, config.TTD_COMPOUND_FILE, config.COMPOUND_FILE)

        load_target_file(config.TTD_TARGET_DOWNLOAD_FILE, config.TTD_TARGET_FILE)
        load_ttd_target_compound_map_file(config.TTD_TARGET_DRUG_MAPPING_DOWNLOAD_FILE,
                                          config.COMPOUND_FILE, config.TTD_COMPOUND_TARGET_MAP_FILE)
        load_disease_file(config.DISEASE_OBO_FILE, config.DISEASE_FILE, config.DISEASE_XREF_FILE)

        load_mesh_descriptor_files()
        load_uberon_anatomy_file(config.UBERON_BASIC_OBO_FILE, config.UBERON_TERM_FILE, config.UBERON_XREF_FILE, config.UBERON_SUBSET_FILE,
                                 config.HUMAN_CONSTRAINTS_FILE, config.MESH_TERM_FILE, config.UBERON_ANATOMY_FILE)

        get_anatomy_pmid_file(
            config.UBERON_ANATOMY_FILE, config.ANATOMY_PMID_FILE, config.REGEN_PMID_FILES)
        get_disease_pmid_file(config.DISEASE_FILE, config.DISEASE_XREF_FILE, config.DISEASE_MESH_XREF_FILE,
                              config.MESH_TERM_FILE, config.DISEASE_PMID_FILE, config.REGEN_PMID_FILES)
        get_disease_anatomy_cooccurrence(config.ANATOMY_PMID_FILE,
                                         config.DISEASE_PMID_FILE, config.DISEASE_ANATOMY_EDGE_FILE)

        log.info(f"Loading data with {config.NO_OF_PROCESSES} thread(s).")
        if config.NO_OF_PROCESSES == 1:
            load_data()
        elif config.NO_OF_PROCESSES > 1:
            load_data_mp(config.NO_OF_PROCESSES, config.BATCH_SIZE)
        else:
            config.NO_OF_PROCESSES
