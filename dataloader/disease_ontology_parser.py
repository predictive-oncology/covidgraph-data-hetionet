import csv
import io
import os

import obonet
import logging
import pandas as pd
from Configs import getConfig

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))


def build_child_dict(ont):

    # first build dictionary of parents
    parent_dict = {}
    for id_, data in ont.nodes(data=True):
        if "is_a" in data:
            parent_dict[id_] = data['is_a']
        else:
            parent_dict[id_] = []

    # build dictionary of children
    child_dict = {}
    for child in parent_dict.keys():
        for parent in parent_dict[child]:
            if not parent in child_dict:
                child_dict[parent] = []
            child_dict[parent].append(child)

    return child_dict


def filter_nodes_by_ancestor(disease_id, child_dict):

    children = []

    if not disease_id in child_dict:
        return [disease_id]
    else:
        for child in child_dict[disease_id]:
            children += [child] + filter_nodes_by_ancestor(child, child_dict)

    return children


def get_infectious_diseases(ont, child_dict):

    infectious_disease_id = "DOID:0050117"
    return filter_nodes_by_ancestor(infectious_disease_id, child_dict)


def parse_ontology_entry(do_id, do_data):

    link = "http://www.disease-ontology.org/?id={0}".format(do_id)
    source = "http://www.disease-ontology.org"
    license = "CC0 1.0"

    disease_def = ''
    if 'def' in do_data:
        disease_def = do_data['def']

    parents = get_disease_parents(do_id, do_data)
    parents = ",".join(parents)

    row = [do_id, do_data['name'], disease_def, parents, link, source, license]

    return row


def parse_ontology_xref(do_id, do_data, xref_list):

    if 'xref' in do_data:
        for xref in do_data['xref']:
            entry = {'doid': do_id, 'xref': xref}
            xref_list.append(entry)


def get_disease_parents(do_id, do_data):

    parents = []

    try:
        for parent in do_data['is_a']:
            parents.append(parent)
    finally:
        return parents


def load_disease_file(disease_download_file, disease_output_file, disease_xref_output_file):

    # parse ontology
    ont = obonet.read_obo(disease_download_file)

    # build child node lookup dictionary
    child_dict = build_child_dict(ont)

    # build filter
    #do_filter = get_infectious_diseases(ont, child_dict)
    do_filter = None

    xref_list = []
    with open(disease_output_file, "w", newline='') as outfile:
        writer = csv.writer(outfile, delimiter="\t")
        writer.writerow(["doid", "name", "definition", "parents", "link", "source", "license"])
        for id_, data in ont.nodes(data=True):
            if do_filter == None or id_ in do_filter:
                row = parse_ontology_entry(id_, data)
                writer.writerow(row)

                parse_ontology_xref(id_, data, xref_list)

    xref_df = pd.DataFrame(xref_list)
    xref_df.to_csv(disease_xref_output_file, sep="\t", index=False)


if __name__ == "__main__":

    load_disease_file(config.DISEASE_OBO_FILE, config.DISEASE_FILE, config.DISEASE_XREF_FILE)
