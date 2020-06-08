import multiprocessing
import os

import py2neo
from Configs import ConfigBase


class DEFAULT(ConfigBase):

    NEO4J_CON = "bolt://184.73.52.96:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASS = "Dulo0416!"

    # commit every n nodes/relations
    COMMIT_INTERVAL = 10000
    # Bundle workloads to <BATCH_SIZE> and load them into database
    # Decrease this number if you RAM is limited on the loading system
    BATCH_SIZE = 300

    # The number of simultaneously working parsing processes
    # You can try using more processes as you have CPU cores / threads, as the loading processes are often waiting for DB loading.
    NO_OF_PROCESSES = multiprocessing.cpu_count() - 1 or 1
    # if one worker fails should we cancel the whole import, or import the rest of the data.
    # you will get feedback on which rows the import failed
    CANCEL_WHOLE_IMPORT_IF_A_WORKER_FAILS = True

    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )

    # if set to True, the dataset will always be downloaded, regardless of its allready existing
    REDOWNLOAD_DATASET_IF_EXISTENT = False

    # Where to store the downloaded dataset
    DATA_BASE_DIR = os.path.join(SCRIPT_DIR, "../dataset/")

    DRUGBANK_HETIONET_DIR = os.path.join(DATA_BASE_DIR, "drugbank")
    DRUGBANK_DOWNLOAD_DIR = os.path.join(DRUGBANK_HETIONET_DIR, "download")

    # Where the dataset csv files from the hetionet Dataset are stored
    DRUGBANK_COMPOUND_FILE = os.path.join(DATA_BASE_DIR, "drugbank-compound-dataset.csv")
    DRUGBANK_XML_FILE = os.path.join(DRUGBANK_DOWNLOAD_DIR, "drugbank.xml.gz")

    DRUGBANK_VOCABULARY_URL = "www.drugbank.ca"
    DRUGBANK_VOCABULARY_URL_PATH = "/releases/5-1-6/downloads/all-drugbank-vocabulary"
    DRUGBANK_VOCABULARY_ZIP_FILE = os.path.join(DRUGBANK_DOWNLOAD_DIR, "drugbank-vocabulary.zip")
    DRUGBANK_VOCABULARY_FILE = os.path.join(DRUGBANK_DOWNLOAD_DIR, "drugbank vocabulary.csv")

    TTD_TARGET_FILE = os.path.join(DATA_BASE_DIR, "target-dataset.csv")
    TTD_COMPOUND_FILE = os.path.join(DATA_BASE_DIR, "ttd-compound-dataset.csv")
    TTD_COMPOUND_TARGET_MAP_FILE = os.path.join(DATA_BASE_DIR, "ttd-compound-target-dataset.csv")

    COMPOUND_FILE = os.path.join(DATA_BASE_DIR, "compound-dataset.csv")

    TTD_TARGET_DOWNLOAD_URL = "http://db.idrblab.net/ttd/sites/default/files/ttd_database/P1-01-TTD_target_download.txt"
    TTD_DRUG_DOWNLOAD_URL = "http://db.idrblab.net/ttd/sites/default/files/ttd_database/P1-02-TTD_drug_download.txt"
    TTD_DRUG_XREF_DOWNLOAD_URL = "http://db.idrblab.net/ttd/sites/default/files/ttd_database/P1-03-TTD_crossmatching.txt"
    TTD_DRUG_SYNONYMS_URL = "http://db.idrblab.net/ttd/sites/default/files/ttd_database/P1-04-Drug_synonyms.txt"
    TTD_TARGET_DRUG_MAPPING_URL = "http://db.idrblab.net/ttd/sites/default/files/ttd_database/P1-07-Drug-TargetMapping.xlsx"
    TTD_DOWNLOAD_DIR = os.path.join(DATA_BASE_DIR, "ttd")
    TTD_TARGET_DOWNLOAD_FILE = os.path.join(TTD_DOWNLOAD_DIR, "ttd_target_download.txt")
    TTD_DRUG_DOWNLOAD_FILE = os.path.join(TTD_DOWNLOAD_DIR, "ttd_drug_download.txt")
    TTD_DRUG_XREF_DOWNLOAD_FILE = os.path.join(TTD_DOWNLOAD_DIR, "ttd_drug_xref_download.txt")
    TTD_TARGET_DRUG_MAPPING_DOWNLOAD_FILE = os.path.join(TTD_DOWNLOAD_DIR, "ttd_drug_target_mapping_download.xlsx")

    DISEASE_FILE = os.path.join(DATA_BASE_DIR, "doid.csv")
    DISEASE_XREF_FILE = os.path.join(DATA_BASE_DIR, "doid-xref.csv")
    DISEASE_MESH_XREF_FILE = os.path.join(DATA_BASE_DIR, "doid-mesh-xref.csv")
    DISEASE_DOWNLOAD_URL = "https://github.com/DiseaseOntology/HumanDiseaseOntology/raw/master/src/ontology/doid.obo"
    DISEASE_DOWNLOAD_DIR = os.path.join(DATA_BASE_DIR, "disease_ontology")
    DISEASE_OBO_FILE = os.path.join(DISEASE_DOWNLOAD_DIR, "doid.obo")

    MESH_DESC_2020_DOWNLOAD_URL = "ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/xmlmesh/desc2020.gz"
    MESH_DESC_2005_DOWNLOAD_URL = "ftp://nlmpubs.nlm.nih.gov/online/mesh/1999-2010/xmlmesh/desc2005.gz"
    MESH_DESC_2002_DOWNLOAD_URL = "ftp://nlmpubs.nlm.nih.gov/online/mesh/1999-2010/xmlmesh/desc2002.gz"
    MESH_DOWNLOAD_DIR = os.path.join(DATA_BASE_DIR, "mesh")
    MESH_DESC_FILE = os.path.join(MESH_DOWNLOAD_DIR, "desc2020.gz")
    MESH_DESC_2005_FILE = os.path.join(MESH_DOWNLOAD_DIR, "desc2005.gz")
    MESH_DESC_2002_FILE = os.path.join(MESH_DOWNLOAD_DIR, "desc2002.gz")
    MESH_TERM_FILE = os.path.join(DATA_BASE_DIR, "mesh-terms.csv")
    MESH_TREE_NUMBER_FILE = os.path.join(DATA_BASE_DIR, "mesh-tree-numbers.csv")
    MESH_TERM_2005_FILE = os.path.join(DATA_BASE_DIR, "mesh-terms-2005.csv")
    MESH_TREE_NUMBER_2005_FILE = os.path.join(DATA_BASE_DIR, "mesh-tree-numbers-2005.csv")
    MESH_TERM_2002_FILE = os.path.join(DATA_BASE_DIR, "mesh-terms-2002.csv")
    MESH_TREE_NUMBER_2002_FILE = os.path.join(DATA_BASE_DIR, "mesh-tree-numbers-2002.csv")

    UBERON_DOWNLOAD_URL = "http://purl.obolibrary.org/obo/uberon.obo"
    UBERON_EXT_DOWNLOAD_URL = "http://purl.obolibrary.org/obo/uberon/ext.obo"
    UBERON_BASIC_DOWNLOAD_URL = "http://purl.obolibrary.org/obo/uberon/basic.obo"
    HUMAN_CONSTRAINTS_URL = "https://raw.github.com/dhimmel/uberon/gh-pages/data/human-constraint.tsv"
    EXT_HUMAN_CONSTRAINTS_URL = "https://raw.github.com/dhimmel/uberon/gh-pages/download/ext_human_constraints.tsv"
    UBERON_DOWNLOAD_DIR = os.path.join(DATA_BASE_DIR, "uberon_ontology")
    UBERON_OBO_FILE = os.path.join(UBERON_DOWNLOAD_DIR, "uberon.obo")
    UBERON_BASIC_OBO_FILE = os.path.join(UBERON_DOWNLOAD_DIR, "basic.obo")
    UBERON_EXT_OBO_FILE = os.path.join(UBERON_DOWNLOAD_DIR, "ext.obo")
    UBERON_TERM_FILE = os.path.join(DATA_BASE_DIR, 'uberon-terms.csv')
    UBERON_XREF_FILE = os.path.join(DATA_BASE_DIR, 'uberon-xref.csv')
    UBERON_SUBSET_FILE = os.path.join(DATA_BASE_DIR, 'uberon-subset.csv')
    HUMAN_CONSTRAINTS_FILE = os.path.join(DATA_BASE_DIR, "human-constraint.tsv")
    EXT_HUMAN_CONSTRAINTS_FILE = os.path.join(DATA_BASE_DIR, "ext_human_constraints.tsv")
    UBERON_ANATOMY_FILE = os.path.join(DATA_BASE_DIR, 'anatomy.csv')

    REGEN_PMID_FILES = False
    DISEASE_PMID_FILE = os.path.join(DATA_BASE_DIR, "disease-pmids.tsv.gz")
    ANATOMY_PMID_FILE = os.path.join(DATA_BASE_DIR, 'anatomy-pmids.tsv.gz')
    DISEASE_ANATOMY_EDGE_FILE = os.path.join(DATA_BASE_DIR, 'disease-anatomy-dataset.csv')

    DISEASE_ASSOCIATES_GENE_URL = "https://github.com/dhimmel/gwas-catalog/raw/master/data/gene-associations.tsv"
    DISEASE_ASSOCIATES_GENE_DIR = os.path.join(DATA_BASE_DIR, "dag")
    DISEASE_ASSOCIATES_GENE_FILE = os.path.join(DISEASE_ASSOCIATES_GENE_DIR, "gene-associations.tsv")

    # Column names, in the 'metadata.csv' file, will be taken over in the created nodes attributes or child nodes.
    # if you are not happy with the names you can overide them here.
    # follow the format from
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html
    # {"old_name":"new_name", "other_column_old_name", "other_column_new_name"}
    METADATA_FILE_COLUMN_OVERRIDE = {
        'Compound': {},
        'Target': {},
        'TargetCompoundMap': {},
        'Disease': {},
        'Anatomy': {"uberon_name": "name"},
        'DiseaseAnatomyMap': {},
        'DiseaseAssociatesGene': {}
    }

    # Define which columns in 'metadata.csv' are properties of a compound
    # they will appear as properties of the :Compound nodes

    ENTITY_PROPERTY_COLUMNS = {
        "Compound": {
            "drugbank_id",
            "ttd_id",
            "accession_numbers",
            "name",
            "cas_number",
            "unii",
            "synonyms",
            "inchi",
            "inchikey",
            "type",
            "groups",
            "atc_codes",
            "categories",
            "description",
            "indication",
            "mechanism",
            "drug_formula",
            "chebi_id",
            "pubchem_id",
            "kegg_id",
            "kegg_drug_id",
            "chemspider_id",
            "drug_smiles",
            "drug_type",
            "highest_stat",
            "company",
            "drug_class",
            "compound_class",
            "therapeutic_class",
            "source",
            "source_url"
        },
        "Target": {
            "ttd_id",
            "name",
            "uniprot_id",
            "type",
            "bio_class",
            "ec_number",
            "function",
            "gene_name",
            "sequence",
            "synonyms",
            "source",
            "license"
        },
        "Disease": {
            "doid",
            "name",
            "definition",
            "link",
            "source",
            "license"
        },
        "Anatomy": {
            "uberon_id",
            "uberon_name",
            "mesh_id",
            "mesh_name",
            "bto_id"
        }
    }

    # Labels that are autocreated based on the json key names (from the full text json files) can be overriden here
    JSON2GRAPH_LABEL_OVERRIDE = {
        "location": "Location",
        "cite_spans": "Citation",
        "affiliation": "Affiliation",
    }

    # if you JSON2GRAPH_GENERATED_HASH_IDS
    JSON2GRAPH_GENERATED_HASH_ID_ATTR_NAME = "_id"
    # Define for which labels and how a hash id attr should be generated
    JSON2GRAPH_GENERATED_HASH_IDS = {
        "Compound": ["drugbank_id", "ttd_id"],
        "Target": ["ttd_id"],
        "Disease": ["doid"],
        "Anatomy": ["uberon_id"]
    }

    JSON2GRAPH_ID_ATTR = {
        "Compound": "compound_id",
        "Target": "target_id",
        "Disease": "disease_id",
        "Anatomy": "anatomy_id"
    }

    JSON2GRAPH_RELTYPE = {
        "DISEASE:GENE": "ASSOCIATES_DaG",
        "DISEASE:DISEASE": "IS_A",
        "COMPOUND:TARGET": ["moa",
                            {
                                "Inhibitor": "IS_INHIBITOR",
                                "Modulates": "IS_MODULATOR",
                                "Antagonist": "IS_ANTAGONIST",
                                "Agonist": "IS_AGONIST",
                                "Binder": "IS_BINDER",
                                "Activator": "IS_ACTIVATOR",
                                "default": "TARGETS"
                            }
                            ],
        "DISEASE:ANATOMY": "LOCALIZES_DlA"
    }

    JSON2GRAPH_CONCAT_LIST_ATTR = {"middle": " "}
    JSON2GRAPH_COLLECTION_NODE_LABEL = "{LIST_MEMBER_LABEL}Collection"
    JSON2GRAPH_COLLECTION_EXTRA_LABELS = ["CollectionHub"]

    def get_graph(self):
        if "GC_NEO4J_URL" in os.environ:
            url = os.getenv("GC_NEO4J_URL")
            if "GC_NEO4J_USER" in os.environ and "GC_NEO4J_PASSWORD" in os.environ:
                user = os.getenv("GC_NEO4J_USER")
                pw = os.getenv("GC_NEO4J_PASSWORD")
                print("URL", url)
                print("pw", pw)
                print("user", user)
                return py2neo.Graph(url, password=pw, user=user)
            return py2neo.Graph(url)
        else:
            return py2neo.Graph(self.NEO4J_CON)


# All following config classes inherit from DEFAULT
class PRODUCTION(DEFAULT):
    pass


class SMOKETEST(DEFAULT):
    DATA_BASE_DIR = os.path.join(DEFAULT.SCRIPT_DIR, "testdataset/")
    METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")


class DEVELOPMENT(DEFAULT):
    # DATA_BASE_DIR = os.path.join(DEFAULT.SCRIPT_DIR, "testdataset/")
    # METADATA_FILE = os.path.join(DATA_BASE_DIR, "metadata.csv")
    pass
