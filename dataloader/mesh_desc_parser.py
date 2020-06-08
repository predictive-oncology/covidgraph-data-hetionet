import os
import pandas as pd
import re
import logging
import gzip
import json
import xml.etree.ElementTree as ET
from Configs import getConfig

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))


def parse_mesh_desc_entries(mesh_desc_file):

    # Read MeSH xml release
    with gzip.open(mesh_desc_file) as xml_file:
        tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract mesh terms
    term_dicts = list()
    for descriptor in root:
        descriptor_ui = descriptor.findtext('DescriptorUI')
        for concept in descriptor.findall('ConceptList/Concept'):
            for term in concept.findall('TermList/Term'):
                term_dict = {
                    'DescriptorUI': descriptor.findtext('DescriptorUI'),
                    'ConceptUI': concept.findtext('ConceptUI'),
                    'TermUI': term.findtext('TermUI'),
                    'TermName': term.findtext('String')
                }
                term_dict.update(concept.attrib)
                term_dict.update(term.attrib)
                term_dicts.append(term_dict)

    #columns = ['DescriptorUI', 'ConceptUI', 'PreferredConceptYN', 'TermUI', 'TermName',
    #           'ConceptPreferredTermYN', 'IsPermutedTermYN', 'LexicalTag', 'PrintFlagYN', 'RecordPreferredTermYN']
    columns = ['DescriptorUI', 'ConceptUI', 'PreferredConceptYN', 'TermUI', 'TermName',
               'ConceptPreferredTermYN', 'IsPermutedTermYN', 'LexicalTag', 'RecordPreferredTermYN']
    term_df = pd.DataFrame(term_dicts)[columns]
    
    return term_df

# Parse MeSH xml release
def parse_mesh_xml(root):
    terms = list()

    for elem in root:
        term = dict()
        term['mesh_id'] = elem.findtext('DescriptorUI')
        term['mesh_name'] = elem.findtext('DescriptorName/String')
        term['semantic_types'] = list({x.text for x in elem.findall(
            'ConceptList/Concept/SemanticTypeList/SemanticType/SemanticTypeUI')})
        term['tree_numbers'] = [x.text for x in elem.findall('TreeNumberList/TreeNumber')]
        terms.append(term)
        
    return terms

def update_mesh_parents(terms):
    
    # Determine ontology parents
    tree_number_to_id = {tn: term['mesh_id'] for term in terms for tn in term['tree_numbers']}
    
    for term in terms:
        parents = set()
        for tree_number in term['tree_numbers']:
            try:
                parent_tn, self_tn = tree_number.rsplit('.', 1)
                parents.add(tree_number_to_id[parent_tn])
            except ValueError:
                pass
        term['parents'] = list(parents)   

# write MeSH terms to json
def write_mesh_json(terms, file_name):
    path = os.path.join('data', file_name)
    with open(path, 'w') as write_file:
        json.dump(terms, write_file, indent=2)

# read MeSH json file
def read_mesh_json(file_name):
    path = os.path.join('data', file_name)
    with open(path) as read_file:
        mesh = json.load(read_file)
        
    return mesh

def get_mesh_tree_numbers(mesh):
    # Extract (mesh_id, mesh_tree_number) pairs
    rows = []
    for term in mesh:
        mesh_id = term['mesh_id']
        mesh_name = term['mesh_name']
        for tree_number in term['tree_numbers']:
            rows.append([mesh_id, mesh_name, tree_number])

    tn_df = pd.DataFrame(rows, columns=['mesh_id', 'mesh_name', 'mesh_tree_number'])
    
    return tn_df

def load_mesh_descriptor_file(mesh_descriptor_input_file, mesh_terms_output_file, mesh_tree_numbers_output_file):

    # Read MeSH xml release
    with gzip.open(mesh_descriptor_input_file) as xml_file:
        tree = ET.parse(xml_file)
    root = tree.getroot()

    terms = parse_mesh_xml(root)
    update_mesh_parents(terms)

    file_base_name = os.path.basename(mesh_descriptor_input_file)
    file_base_name = os.path.splitext(file_base_name)[0]
    output_dir = os.path.dirname(mesh_terms_output_file)

    mesh_descriptor_json_file = os.path.join(output_dir, "mesh_{0}.json".format(file_base_name))
    write_mesh_json(terms, mesh_descriptor_json_file)
    mesh = read_mesh_json(mesh_descriptor_json_file)

    mesh_df = pd.DataFrame.from_dict(mesh)[['mesh_id', 'mesh_name']]
    mesh_df.to_csv(mesh_terms_output_file, sep='\t', index=False)

    tn_df = get_mesh_tree_numbers(mesh)
    tn_df.to_csv(mesh_tree_numbers_output_file, sep='\t', index=False)


def load_mesh_descriptor_files():

    input_files = [config.MESH_DESC_FILE, config.MESH_DESC_2005_FILE, config.MESH_DESC_2002_FILE]
    terms_output_files = [config.MESH_TERM_FILE, config.MESH_TERM_2005_FILE, config.MESH_TERM_2002_FILE]
    tree_numbers_output_files = [config.MESH_TREE_NUMBER_FILE, config.MESH_TREE_NUMBER_2005_FILE, config.MESH_TREE_NUMBER_2002_FILE]

    for (input_file, term_output_file, tree_number_output_file) in zip(input_files, terms_output_files, tree_numbers_output_files):
        load_mesh_descriptor_file(input_file, term_output_file, tree_number_output_file)        

    
if __name__ == "__main__":

    load_mesh_descriptor_files()
