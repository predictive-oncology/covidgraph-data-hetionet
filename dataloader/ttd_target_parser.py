import logging
import os
import re

import openpyxl
import pandas as pd
from Configs import getConfig

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))


def parse_targets(ttd_target_download_file):

    pattern = re.compile(r"^(T\d{5})\t(.*?)\t(.*)$")

    target_main_keys = ['Name', 'UniProt ID', 'Type of Target',
                        'EC Number', 'BioChemical Class', 'Function', 'Target Validation']
    drug_modes_of_action = ['Modulator', 'Inhibitor', 'Agonist', 'Antagonist', 'Binder', 'Activator', 'Stimulator', 'Cofactor', 'Modulator (allosteric modulator)',
                            'Blocker', 'Blocker (channel blocker)', 'Inducer', 'Inhibitor (gating inhibitor)',  'Suppressor', 'Regulator (upregulator)',
                            'Breaker', 'Immunomodulator', 'Regulator', 'Opener', 'Stabilizer', 'Enhancer', 'Binder (minor groove binder)', 'Intercalator',
                            'Immunomodulator (Immunostimulant)', 'Stablizer'
                            ]

    prev_target_id = None
    targets = dict()
    with open(ttd_target_download_file, 'rt') as in_file:
        for linenum, line in enumerate(in_file):
            match = pattern.search(line)

            if match != None:

                target_id = match.group(1)
                key = match.group(2)
                value = match.group(3)

                #value = value.split("\t")

                if prev_target_id != target_id:
                    if prev_target_id in targets.keys():
                        print("Error: Target {0} already added".format(prev_target_id))
                    elif prev_target_id != None:
                        targets[prev_target_id] = target

                    # initialize new target dict
                    target = {'ID': target_id}

                # create drug key to store mode of action
                if key == "DRUGINFO":
                    (ttd_drug_id, drug_name, drug_status) = value.split("\t")
                    if "DRUGINFO" not in target:
                        target["DRUGINFO"] = dict()
                    target["DRUGINFO"][ttd_drug_id] = [drug_name, drug_status]

                # check if current key is a drug mode of action
                elif key in drug_modes_of_action:
                    # set drug mode of action
                    drug_key = [k for k, v in target["Drugs"].items() if k.lower() == value.lower()]
                    if len(drug_key) == 1:
                        target["Drugs"][drug_key[0]] = key
                        target["Drug"].append(drug_key[0])
                        target["DrugMethod"].append(key)
                        target["DrugStatus"].append()
                    else:
                        print("Error: Drug {0} not found in target.".format(value))

                elif key in target:
                    if type(target[key]) != list:
                        target[key] = [target[key]]

                    if value not in target[key]:
                        target[key].append(value)
                else:
                    target[key] = value

                prev_target_id = target_id

    targets[target_id] = target

    return targets


def parse_drug(ttd_drug_download_file):

    key_map = {
        "TRADNAME": "trade_name",
        "DRUGCOMP": "company",
        "THERCLAS": "therapeutic_class",
        "DRUGTYPE": "drug_type",
        "DRUGINCH": "inchi",
        "DRUGINKE": "inchikey",
        "DRUGSMIL": "drug_smiles",
        "HIGHSTAT": "highest_stat",
        "DRUGCLAS": "drug_class",
        "DRUADIID": "drug_adi_id",
        "COMPCLAS": "compound_class"
    }

    pattern = re.compile(r"^(D.*)\t(.*?)\t(.*)$")

    prev_drug_id = None
    drugs = {}
    drug = {}

    with open(ttd_drug_download_file, 'rt') as in_file:

        for linenum, line in enumerate(in_file):
            line = line.strip()

            match = pattern.search(line)

            if match != None:

                drug_id = match.group(1)
                key = match.group(2)
                value = match.group(3)

                #print("Drug id = {0}, key = {1}, value = {2}".format(drug_id, key, value))
                if prev_drug_id != drug_id:
                    if prev_drug_id in drugs.keys():
                        print("Error: drug {} already added".format(prev_drug_id))
                    else:
                        drugs[prev_drug_id] = drug

                    drug = {'ttd_id': drug_id}

                if key != 'DRUG__ID':
                    drug[key_map[key]] = value

                prev_drug_id = drug_id

        drugs[drug_id] = drug

    return drugs


def post_process_value(key, value):

    if key == "cas_number":
        return value.replace("CAS ", "")
    else:
        return value


def parse_drug_xref(ttd_drug_xref_file):

    key_map = {
        "DRUGNAME": "name",
        "CASNUMBE": "cas_number",
        "D_FOMULA": "drug_formula",
        "PUBCHCID": "pubchem_cid",
        "PUBCHSID": "pubchem_sid",
        "CHEBI_ID": "chebi_id",
        "SUPDRATC": "superdrug_atc",
        "SUPDRCAS": "superdrug_cas"
    }
    pattern = re.compile(r"^(D.*)\t(.*?)\t(.*)$")

    prev_drug_id = None
    drug_xrefs = {}
    drug = {}

    with open(ttd_drug_xref_file, 'rt') as in_file:

        for linenum, line in enumerate(in_file):
            line = line.strip()

            match = pattern.search(line)

            if match != None:

                drug_id = match.group(1)
                key = match.group(2)
                value = match.group(3)

                #print("Drug id = {0}, key = {1}, value = {2}".format(drug_id, key, value))
                if prev_drug_id != drug_id:
                    if prev_drug_id in drug_xrefs.keys():
                        print("Error: drug {} already added".format(prev_drug_id))
                    else:
                        drug_xrefs[prev_drug_id] = drug

                    drug = {'ttd_id': drug_id}

                if key != 'TTDDRUID':
                    mapped_key = key_map[key]
                    drug[mapped_key] = post_process_value(mapped_key, value)

                prev_drug_id = drug_id

        drug_xrefs[drug_id] = drug

    return drug_xrefs


def parse_drug_targets(drug_target_map_download_file):

    wb_obj = openpyxl.load_workbook(drug_target_map_download_file)

    sheet = wb_obj.active

    drug_targets = []

    for i, row in enumerate(sheet.iter_rows(values_only=True)):

        if i == 0:
            continue

        drug_target = {}
        drug_target['target_id'] = row[0]
        drug_target['drug_id'] = row[2]
        drug_target['moa'] = row[5]
        drug_target['activity'] = row[6]
        drug_target['reference'] = row[7]

        if drug_target['moa'] == ".":
            drug_target['moa'] = ""

        drug_targets.append(drug_target)

    return drug_targets


def load_ttd_compound_file(drug_download_file, drug_xref_file, drug_output_file):

    drug = parse_drug(drug_download_file)
    drug_xref = parse_drug_xref(drug_xref_file)

    drug_df = pd.DataFrame.from_dict(drug, orient='index')
    drug_xref_df = pd.DataFrame.from_dict(drug_xref, orient='index')

    # drugbank columns
    # columns = ['ttd_id', 'name', 'type', 'groups', 'cas_number', 'atc_codes', 'categories', 'inchikey', 'inchi', 'description', 'indication', 'mechanism',
    #           'chebi_id', 'pubchem_id', 'kegg_id', 'kegg_drug_id', 'chemspider_id', 'license', 'source']

    drug_merged_df = drug_df.join(drug_xref_df, on="ttd_id", rsuffix="_xref")
    drug_merged_df['source_id'] = drug_merged_df['ttd_id']
    drug_merged_df['source'] = 'TTD'
    drug_merged_df['source_url'] = "http://db.idrblab.net/ttd/data/drug/details/" + drug_merged_df["ttd_id"]
    drug_merged_df['license'] = 'CC 1.0'
    drug_merged_df.to_csv(drug_output_file, sep='\t', index=False)

    return drug_merged_df


def load_target_file(target_download_file, target_output_file):

    targets = parse_targets(target_download_file)

    columns = ["ID", "TARGNAME", "UNIPROID", "TARGTYPE", "BIOCLASS",
               "ECNUMBER", "FUNCTION", "GENENAME", "SEQUENCE", "SYNONYMS"]
    targets_df = pd.DataFrame.from_dict(targets, orient='index')
    targets_df = targets_df[columns]
    targets_df.columns = ["ttd_id", "name", "uniprot_id", "type", "bio_class",
                          "ec_number", "function", "gene_name", "sequence", "synonyms"]
    targets_df['source'] = "http://db.idrblab.net/ttd/"
    targets_df['license'] = 'CC 1.0'
    targets_df.to_csv(target_output_file, sep='\t', index=False)


def load_ttd_target_compound_map_file(drug_target_map_download_file, compound_output_file, drug_target_map_output_file):

    drug_target_map = parse_drug_targets(drug_target_map_download_file)
    drug_target_df = pd.DataFrame(drug_target_map)
    drug_target_df.columns = ['ttd_id', 'ttd_drug_id', 'moa', 'activity', 'reference']
    log.debug("Loaded {0} rows from drug/target mapping file.".format(drug_target_df.shape[0]))

    compound_df = pd.read_csv(compound_output_file, sep="\t")
    compound_df = compound_df[['drugbank_id', 'ttd_id']]
    compound_df.columns = ['drugbank_id', 'ttd_drug_id']
    log.debug("Loaded {0} rows from compound file".format(compound_df.shape[0]))

    drug_target_df = drug_target_df.merge(compound_df)

    drug_target_df['source'] = "http://db.idrblab.net/ttd/"
    drug_target_df['license'] = 'CC 1.0'
    drug_target_df.to_csv(drug_target_map_output_file, sep='\t', index=False)


if __name__ == "__main__":

    config = getConfig()

    #load_target_file(download_path, target_output_file)
    load_ttd_target_compound_map_file(config.TTD_TARGET_DRUG_MAPPING_DOWNLOAD_FILE,
                                      config.COMPOUND_FILE, config.TTD_COMPOUND_TARGET_MAP_FILE)
