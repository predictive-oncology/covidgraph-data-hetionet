import os
import sys
import logging
import pandas as pd
from Configs import getConfig
import urllib.request
import shutil
from drugbank_parser import load_drugbank_file, load_drugbank_vocab_file
from ttd_target_parser import load_ttd_compound_file

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))

def load_compounds(drugbank_vocabulary_file, drugbank_xml_file, ttd_compound_file, ttd_compound_xref_file, 
                   drugbank_output_file, ttd_output_file, compound_output_file):

    # DrugBank vocabulary data set released under a Creative Common’s CC0 International License.
    drugbank_vocab_df = load_drugbank_vocab_file(drugbank_vocabulary_file)
    drugbank_vocab_df = drugbank_vocab_df.set_index('drugbank_id', drop=False)
    log.debug("Vocab df shape = {0}".format(drugbank_vocab_df.shape[0]))
    #print(drugbank_vocab_df[drugbank_vocab_df.drugbank_id == "DB12619"][['drugbank_id', 'source', 'source_url']])

    # Part of hetionet 2015 release
    drugbank_df = load_drugbank_file(drugbank_vocabulary_file, drugbank_xml_file, drugbank_output_file)
    drugbank_df = drugbank_df.set_index('drugbank_id', drop=False)
    log.debug("Drubank df shape = {0}".format(drugbank_df.shape[0]))

    # Merge two data sets on drugbank id
    drugbank_merged_df = drugbank_vocab_df.join(drugbank_df, rsuffix="_2015")
    drugbank_df = drugbank_merged_df
    log.debug("Drugbank merged df shape = {0}".format(drugbank_df.shape[0]))

    drugbank_cas_df = drugbank_df[drugbank_df.cas_number.notnull()]
    drugbank_pubchem_df = drugbank_df[drugbank_df.pubchem_id.notnull()]
    log.debug("DrugBank row count = {0}".format(drugbank_df.shape[0]))    
    log.debug("DrugBank compounds with CAS number = {0}".format(drugbank_cas_df.shape[0]))
    log.debug("DrugBank compounds with pubchem id = {0}".format(drugbank_pubchem_df.shape[0]))

    ttd_df = load_ttd_compound_file(ttd_compound_file, ttd_compound_xref_file, ttd_output_file)

    ttd_cas_df = ttd_df[ttd_df.cas_number.notnull()]
    ttd_pubchem_df = ttd_df[ttd_df.pubchem_cid.notnull()]
    log.debug("TTD row count = {0}".format(ttd_df.shape[0]))    
    log.debug("TTD compounds with CAS number = {0}".format(ttd_cas_df.shape[0]))
    log.debug("TTD compounds with pubchem id = {0}".format(ttd_pubchem_df.shape[0]))

    # ttd contains many different compounds with same CAS number
    # remove any entries that match on CAS number, but differ on pubchem_id (only if both pubchem_id entries exist)
    compound_cas_number_df = drugbank_cas_df.set_index('cas_number', drop=False).join(ttd_cas_df.set_index('cas_number', drop=False), how="inner", rsuffix="_ttd")
    log.debug("Mapped DrugBank/TTD compounds by CAS number (before removal) = {0}".format(compound_cas_number_df.shape[0]))
    compound_cas_number_df = compound_cas_number_df[compound_cas_number_df.pubchem_id.isna() | compound_cas_number_df.pubchem_cid.isna() | (compound_cas_number_df.pubchem_id == compound_cas_number_df.pubchem_cid)]
    compound_cas_number_df = compound_cas_number_df[['drugbank_id', 'ttd_id']]
    log.debug("Mapped DrugBank/TTD compounds by CAS number (after removal) = {0}".format(compound_cas_number_df.shape[0]))

    compound_pubchem_df = drugbank_pubchem_df.set_index('pubchem_id', drop=False).join(ttd_pubchem_df.set_index('pubchem_cid', drop=False), how="inner", rsuffix="_ttd")
    compound_pubchem_df = compound_pubchem_df[['drugbank_id', 'ttd_id']]
    log.debug("Mapped DrugBank/TTD compounds by pubchem id = {0}".format(compound_pubchem_df.shape[0]))

    compound_map_df = pd.concat([compound_cas_number_df, compound_pubchem_df], ignore_index = True)
    compound_map_df = compound_map_df.drop_duplicates()
    log.debug("Distinct mapped DrugBank/TTD pairs = {0}".format(compound_map_df.shape[0]))


    # compound data set =
    # 1. matched compounds from TTD, DrugBank

    # check for duplicate drugbank mappings
    dup_drugbank_df = compound_map_df[compound_map_df.duplicated(['drugbank_id'])]
    if dup_drugbank_df.shape[0] > 0:
        dup_drugbank_list = dup_drugbank_df.drugbank_id.tolist()
        compound_map_df = compound_map_df[~compound_map_df.drugbank_id.isin(dup_drugbank_list)]

    # check for duplicate ttd mappings
    dup_ttd_df = compound_map_df[compound_map_df.duplicated(['ttd_id'])]
    if dup_ttd_df.shape[0] > 0:
        dup_ttd_list = dup_ttd_df.ttd_id.tolist()
        compound_map_df = compound_map_df[~compound_map_df.ttd_id.isin(dup_ttd_list)]
    log.debug("Distinct mapped DrugBank/TTD pairs after removal of duplicates = {0}".format(compound_map_df.shape[0]))

    # add mapped ttd entries to drugbank df
    log.debug("Drugbank dataframe row count = {0}".format(drugbank_df.shape[0]))

    #print(drugbank_df[drugbank_df.groupby('drugbank_id').cumcount() > 1].shape)
    compound_df = drugbank_df.join(compound_map_df.set_index('drugbank_id'), how="left")
    log.debug("Drugbank dataframe row count = {0}".format(compound_df.shape[0]))

    #join ttd dataset
    compound_df = compound_df.set_index("ttd_id").join(ttd_df.set_index("ttd_id"), how="left", rsuffix="_ttd").reset_index(drop=False)
    log.debug("DrugBank mapped to TTD compounds.  Row count = {0}".format(compound_df.shape[0]))

    # Map TTD features to DrugBank features (if they don't exist)
    compound_df.loc[compound_df.inchikey.isna(), 'inchikey'] = compound_df[compound_df.inchikey.isna()].inchikey_ttd
    compound_df.loc[compound_df.inchi.isna(), 'inchi'] = compound_df[compound_df.inchi.isna()].inchi_ttd
    compound_df.loc[compound_df.chebi_id.isna(), 'chebi_id'] = compound_df[compound_df.chebi_id.isna()].chebi_id_ttd
    compound_df.loc[compound_df.ttd_id.notnull(), 'source'] = compound_df[compound_df.ttd_id.notnull()].source + ", " + compound_df[compound_df.ttd_id.notnull()].source_ttd
    compound_df.loc[compound_df.ttd_id.notnull(), "source_url"] = compound_df[compound_df.ttd_id.notnull()].source_url + ", " + compound_df[compound_df.ttd_id.notnull()].source_url_ttd

    columns = ["drugbank_id", "ttd_id", "accession_numbers", "name", "cas_number", "unii", "synonyms", "inchi", "inchikey",
               "type", "groups", "atc_codes", "categories", "description", "indication", "mechanism", "drug_formula",
               "chebi_id", "pubchem_id", "kegg_id", "kegg_drug_id", "chemspider_id", 
               "drug_smiles", "drug_type",
               "highest_stat", "company", "drug_class", "compound_class", "therapeutic_class", "source", "source_url"]
    compound_df = compound_df[columns]

    # unmatched compounds from TTD
    mapped_ttd_compounds = compound_map_df.ttd_id.tolist()
    unmapped_ttd_df = ttd_df[~ttd_df.ttd_id.isin(mapped_ttd_compounds)]
    log.debug("Unmapped TTD compounds = {0}".format(unmapped_ttd_df.shape[0]))

    column_filter = ["ttd_id", "name", "cas_number", "inchi", "inchikey", "drug_type",
                     "highest_stat", "drug_class", "company", "compound_class", "therapeutic_class",
                     "drug_formula", "pubchem_cid", "chebi_id", "source_url", "source"]
    unmapped_ttd_df = unmapped_ttd_df[column_filter]
    unmapped_ttd_df.columns = ["ttd_id", "name", "cas_number", "inchi", "inchikey", "drug_type",
                     "highest_stat", "drug_class", "company", "compound_class", "therapeutic_class",
                     "drug_formula", "pubchem_id", "chebi_id", "source_url", "source"]


    compound_df = pd.concat([compound_df, unmapped_ttd_df])
    log.debug("Compound row count = {0}".format(compound_df.shape[0]))

    compound_df.to_csv(compound_output_file, sep='\t', index=False)


if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__)))
    )
    SCRIPT_DIR = os.path.join(SCRIPT_DIR, "..")
    sys.path.append(os.path.normpath(SCRIPT_DIR))


if __name__ == "__main__":

    from dataloader.download_data import download

    download()

    config = getConfig()

    load_compounds(config.DRUGBANK_VOCABULARY_FILE, config.DRUGBANK_XML_FILE, config.TTD_DRUG_DOWNLOAD_FILE, config.TTD_DRUG_XREF_DOWNLOAD_FILE, 
                   config.DRUGBANK_COMPOUND_FILE, config.TTD_COMPOUND_FILE, config.COMPOUND_FILE)




