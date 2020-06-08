import logging
import os
import shutil
import urllib.request
import zipfile

from Configs import getConfig

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))


def get_drugbank_download_url(url, url_path):

    import urllib.request
    import http.client

    conn = http.client.HTTPSConnection(url)
    conn.request("GET", url_path)
    response = conn.getresponse()
    download_url = response.msg['Location']

    return download_url


def download_file(url, dest_file):

    log.info("downloading {0} to {1}".format(url, dest_file))
    with urllib.request.urlopen(url) as response:
        with open(dest_file, "wb") as out_file:
            shutil.copyfileobj(response, out_file)


def download_ftp_file(url, dest_file):

    log.info("downloading {0} to {1}".format(url, dest_file))
    urllib.request.urlretrieve(url, dest_file)


def download_drugbank_dataset():

    log.info("downloading DrugBank compound dataset")
    download_url = get_drugbank_download_url(config.DRUGBANK_VOCABULARY_URL, config.DRUGBANK_VOCABULARY_URL_PATH)

    if not os.path.isdir(config.DRUGBANK_DOWNLOAD_DIR):
        os.makedirs(config.DRUGBANK_DOWNLOAD_DIR)

    download_file(download_url, config.DRUGBANK_VOCABULARY_ZIP_FILE)

    with zipfile.ZipFile(config.DRUGBANK_VOCABULARY_ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall(config.DRUGBANK_DOWNLOAD_DIR)


def download_disease_dataset():

    log.info("downloading disease ontology dataset")
    if not os.path.isdir(config.DISEASE_DOWNLOAD_DIR):
        os.makedirs(config.DISEASE_DOWNLOAD_DIR)

    download_file(config.DISEASE_DOWNLOAD_URL, config.DISEASE_OBO_FILE)


def download_uberon_dataset():

    log.info("downloading uberon ontology dataset")
    if not os.path.isdir(config.UBERON_DOWNLOAD_DIR):
        os.makedirs(config.UBERON_DOWNLOAD_DIR)

    download_file(config.HUMAN_CONSTRAINTS_URL, config.HUMAN_CONSTRAINTS_FILE)
    download_file(config.UBERON_DOWNLOAD_URL, config.UBERON_OBO_FILE)
    download_file(config.UBERON_BASIC_DOWNLOAD_URL, config.UBERON_BASIC_OBO_FILE)
    download_file(config.UBERON_EXT_DOWNLOAD_URL, config.UBERON_EXT_OBO_FILE)


def download_mesh_descriptors():

    log.info("downloading mesh descriptor dataset")
    if not os.path.isdir(config.MESH_DOWNLOAD_DIR):
        os.makedirs(config.MESH_DOWNLOAD_DIR)

    download_ftp_file(config.MESH_DESC_2020_DOWNLOAD_URL, config.MESH_DESC_FILE)
    download_ftp_file(config.MESH_DESC_2005_DOWNLOAD_URL, config.MESH_DESC_2005_FILE)
    download_ftp_file(config.MESH_DESC_2002_DOWNLOAD_URL, config.MESH_DESC_2002_FILE)


def download_ttd_dataset():

    log.info("downloading ttd dataset")
    if not os.path.isdir(config.TTD_DOWNLOAD_DIR):
        os.makedirs(config.TTD_DOWNLOAD_DIR)

    download_file(config.TTD_TARGET_DOWNLOAD_URL, config.TTD_TARGET_DOWNLOAD_FILE)
    download_file(config.TTD_DRUG_DOWNLOAD_URL, config.TTD_DRUG_DOWNLOAD_FILE)
    download_file(config.TTD_DRUG_XREF_DOWNLOAD_URL, config.TTD_DRUG_XREF_DOWNLOAD_FILE)
    download_file(config.TTD_TARGET_DRUG_MAPPING_URL, config.TTD_TARGET_DRUG_MAPPING_DOWNLOAD_FILE)


def download_disease_gene_associations():
    if not os.path.isdir(config.DISEASE_ASSOCIATES_GENE_DIR):
        os.makedirs(config.DISEASE_ASSOCIATES_GENE_DIR)

    download_file(config.DISEASE_ASSOCIATES_GENE_URL, config.DISEASE_ASSOCIATES_GENE_FILE)


def download():

    if not os.path.isdir(config.DATA_BASE_DIR):
        os.makedirs(config.DATA_BASE_DIR)
    if not config.REDOWNLOAD_DATASET_IF_EXISTENT:
        if os.path.isfile(config.DRUGBANK_COMPOUND_FILE):
            log.info(
                "Skip downloading dataset. Seems to be already existing. Switch 'REDOWNLOAD_DATASET_IF_EXISTENT' to True to force download."
            )
            return

    log.info("Start downloading DrugBank Dataset...")
    download_drugbank_dataset()
    log.info("Finished downloading DrugBank Dataset...")

    log.info("Start downloading TTD Dataset...")
    download_ttd_dataset()
    log.info("Finished downloading TTD Dataset...")

    log.info("Start downloading disease ontology Dataset...")
    download_disease_dataset()
    log.info("Finished downloading disease ontology Dataset...")

    log.info("Start downloading mesh descriptor Dataset")
    download_mesh_descriptors()
    log.info("Finished downloading mesh descriptor Dataset")

    log.info("Start downloading uberon ontology Dataset...")
    download_uberon_dataset()
    log.info("Finished downloading uberon ontology Dataset...")
    log.info("Start downloading disease gene associations Dataset...")
    download_disease_gene_associations()
    log.info("Finished downloading disease gene associations Dataset...")


if __name__ == "__main__":
    download()
