import os
import pandas as pd
import re
import logging
import obo
from Configs import getConfig

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))


def parse_anatomy_entries(basic_obo_file):

    with open(basic_obo_file) as basic_file:
        basic = obo.read_obo(basic_file)

    # Extract information from the graph
    term_rows = []
    xref_rows = []
    subset_rows = []

    for node, data in basic.nodes(data=True):

        # obo read_obo parses "relationship" attributes as graph edges
        # mostly, these are relationships to another Uberon term
        # sometimes, these are "depicted_by" relationships that refer to an image url
        # which should not be added to the graph (it creates nodes for the image url entry which will not contain any data)
        try:
            data = data['attr_dict']

            term_rows.append((node, data['name']))
        except Exception as e:
            continue

        for xref in data.get('xref', []):
            xref_rows.append((node, xref))

        for subset in data.get('subset', []):
            subset_rows.append((node, subset))

    term_df = pd.DataFrame(term_rows, columns=['uberon_id', 'uberon_name']).sort_values(['uberon_id', 'uberon_name'])
    xref_df = pd.DataFrame(xref_rows, columns=['uberon_id', 'xref']).sort_values(['uberon_id', 'xref'])
    subset_df = pd.DataFrame(subset_rows, columns=['uberon_id', 'subset']).sort_values(['uberon_id', 'subset'])

    return (term_df, xref_df, subset_df)


def update_xrefs(df):

    # https://www.nlm.nih.gov/mesh/intro_trees.html : The numbers are subject to change 
    # when new descriptors are added or the hierarchical arrangement is revised to reflect vocabulary changes.

    # TODO: compile a lookup table that includes legacy and current tree numbers 
    # from all MeSH releases instead of including lookups from 2002 & 2005

    # Update MESH IDs that are tree numbers
    tree_number_df = pd.read_table(config.MESH_TREE_NUMBER_FILE)
    tree_number_2002_df = pd.read_table(config.MESH_TREE_NUMBER_2005_FILE)
    tree_number_2005_df = pd.read_table(config.MESH_TREE_NUMBER_2002_FILE)

    tn_to_id = dict(zip(tree_number_df.mesh_tree_number, tree_number_df.mesh_id))
    tn_to_id_2002 = dict(zip(tree_number_2002_df.mesh_tree_number, tree_number_2002_df.mesh_id))
    tn_to_id_2005 = dict(zip(tree_number_2005_df.mesh_tree_number, tree_number_2005_df.mesh_id))

    # outdated MeSH xrefs mapped to current MeSH descriptors
    xref_map = {'A03.492' : 'D041981',
                'A14.254.245' : 'D014094',
                'A03.867' : 'D010614',
                'A03.867.490' : 'D007013',
                'A03.867.557' : 'D009305',
                'A03.867.603' : 'D009960',
                'A03.867.603.925' : 'D014066'}

    def update_xref(x):
        try:
            vocab, identifier = x.split(':', 1)
            if vocab == 'MESH':
                if re.search('D[0-9]{6}', identifier):
                    if identifier in xref_map:
                        log.debug("Found mapped identifier: {0}".format(identifier))
                        return 'MESH:' + xref_map.get(identifier)
                    return x   
                return 'MESH:' + (xref_map.get(identifier) or tn_to_id.get(identifier) or tn_to_id_2005.get(identifier) or tn_to_id_2002.get(identifier) or identifier)
        except Exception as e:
            pass
        
        return x

    df.xref = df.xref.map(update_xref)
    return df

def load_uberon_anatomy_file(basic_obo_file, anatomy_term_file, anatomy_xref_file, anatomy_subset_file, 
                             human_constraints_file, mesh_term_file, anatomy_output_file):

    (term_df, xref_df, subset_df) = parse_anatomy_entries(basic_obo_file)
    term_df.to_csv(anatomy_term_file, sep="\t", index=False)

    # Create a dataframe of cross-references
    xref_df = update_xrefs(xref_df)
    xref_df.to_csv(anatomy_xref_file, sep='\t', index=False)
    
    # Create a dataframe of term subsets
    subset_df.to_csv(anatomy_subset_file, sep='\t', index=False)
    subset_dict = {subset: set(df.uberon_id) for subset, df in subset_df.groupby('subset')}

    # create 'slim' data set
    # * potentially human-relevant (definitively non-human terms are removed)
    # * in uberon_slim, pheno_slim
    # * not in non_informative, upper_level, grouping_class
    # * has a MeSH cross-reference

    # only include terms where no negative evidence exists that term is human
    human_df = pd.read_table(human_constraints_file)
    human_ids = set(human_df.query('no_negative_evidence == 1').uberon_id)
    uberon_slim_df = term_df[term_df.uberon_id.isin(human_ids)].merge(xref_df)

    # only include terms with MeSH xref
    uberon_slim_df['mesh_id'] = uberon_slim_df.xref.map(lambda x: x.split(':', 1)[1] if x and x.startswith('MESH:') else '')
    uberon_slim_df = uberon_slim_df[uberon_slim_df.mesh_id != ''].drop('xref', 1)

    # exclude certain subsets
    exclude = subset_dict['non_informative'] | subset_dict['upper_level'] | subset_dict['grouping_class']    
    uberon_slim_df = uberon_slim_df[-uberon_slim_df.uberon_id.isin(exclude)]

    # filter by uberon_slim and pheno_slim subsets
    uberon_slim_df = uberon_slim_df[uberon_slim_df.uberon_id.isin(subset_dict['uberon_slim'] | subset_dict['pheno_slim'])]

    # Add mesh_name column to uberon dataframe
    mesh_df = pd.read_table(mesh_term_file)
    uberon_slim_df = uberon_slim_df.merge(mesh_df, how='left')

    # mesh_id_str = "|".join(["D008198", "D001365", "D006119", "D009333", "D008643"])
    # mesh_df[mesh_df.mesh_id.str.contains(mesh_id_str)]

    uberon_slim_df.loc[uberon_slim_df.mesh_id == "D008198|D001365", "mesh_name"] = "Lymph Nodes|Axilla"
    uberon_slim_df.loc[uberon_slim_df.mesh_id == "D008198|D006119", "mesh_name"] = "Lymph Nodes|Groin"
    uberon_slim_df.loc[uberon_slim_df.mesh_id == "D008198|D009333", "mesh_name"] = "Lymph Nodes|Neck"
    uberon_slim_df.loc[uberon_slim_df.mesh_id == "D008198|D008643", "mesh_name"] = "Lymph Nodes|Mesentery"

    # Add BTO cross-references. Assumes that uberon-to-bto relationships are one-to-one, which is occaisionally not true.
    bto_df = xref_df[xref_df.xref.str.startswith('BTO:').fillna(False)]
    bto_df = bto_df.rename(columns={'xref': 'bto_id'})
    bto_df = bto_df[bto_df.uberon_id.isin(uberon_slim_df.uberon_id)]
    uberon_slim_df = uberon_slim_df.merge(bto_df, how='left').drop_duplicates('uberon_id')

    # Save as a tsv
    uberon_slim_df.to_csv(anatomy_output_file, index=False, sep='\t')

if __name__ == "__main__":

    load_uberon_anatomy_file(config.UBERON_BASIC_OBO_FILE, config.UBERON_TERM_FILE, config.UBERON_XREF_FILE, config.UBERON_SUBSET_FILE, 
                             config.HUMAN_CONSTRAINTS_FILE, config.MESH_TERM_FILE, config.UBERON_ANATOMY_FILE)