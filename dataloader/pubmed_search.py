import io
import os
import gzip
import pandas as pd
import requests
import networkx
import time
import logging
import itertools
import scipy.stats
import xml.etree.ElementTree as ET
from Configs import getConfig

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))

# Reference:  https://github.com/dhimmel/medline/blob/gh-pages/cooccurrence.py

def esearch_query(payload, retmax = 100, sleep=2):
    """
    Query the esearch E-utility.
    """
    url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
    payload['retmax'] = retmax
    payload['retstart'] = 0
    ids = list()
    count = 1
    while payload['retstart'] < count:
        response = requests.get(url, params=payload)
        xml = ET.fromstring(response.content)
        count = int(xml.findtext('Count'))
        ids += [xml_id.text for xml_id in xml.findall('IdList/Id')]
        payload['retstart'] += retmax
        time.sleep(sleep)
    return ids

def make_gzip_pmid_file(rows, output_file):

    df = pd.DataFrame(rows)

    with gzip.open(output_file, 'w') as write_file:
        write_file = io.TextIOWrapper(write_file)
        df.to_csv(write_file, sep='\t', index=False)

def download_disease_pmids(disease_file, disease_xref_file, disease_mesh_xref_file, mesh_term_file, disease_pmid_file):

    disease_df = pd.read_table(disease_file, sep="\t")
    disease_xref_df = pd.read_table(disease_xref_file, sep="\t")

    disease_mesh_xref_df = disease_xref_df[disease_xref_df.xref.str.startswith('MESH:')]
    disease_mesh_xref_df['mesh_id'] = disease_mesh_xref_df.xref.map(lambda x: x.split(':', 1)[1] if x and x.startswith('MESH:') else '')
    disease_mesh_xref_df = disease_mesh_xref_df[disease_mesh_xref_df.mesh_id != ''].drop('xref', 1)

    # merge mesh_id xref
    disease_df = disease_df.merge(disease_mesh_xref_df, how='left')

    # manually add missing mesh_id xrefs (list should be expanded)
    disease_df.loc[disease_df.doid == "DOID:0080642", "mesh_id"] = "D065207"
    disease_df.loc[disease_df.doid == "DOID:0080599", "mesh_id"] = "D018352"

    # merge mesh names
    mesh_df = pd.read_table(mesh_term_file)
    disease_df = disease_df.merge(mesh_df, how='left')

    # manually add supplementary concepts (list should be expanded)
    disease_df.loc[disease_df.doid == "DOID:0080600", "mesh_sc_id"] = "C000657245"
    disease_df.loc[disease_df.doid == "DOID:0080600", "mesh_sc_name"] = "COVID-19"
    
    # output intermediate file mapping disease id's to mesh id's (and names)
    disease_df.to_csv(disease_mesh_xref_file, sep="\t")

    rows_out = list()

    for name, mesh_term_group in disease_df.groupby(['doid', 'name']):
        
        term_query = ''
        mesh_ids = []
        mesh_names = []
        for i, term in mesh_term_group.iterrows():
            if not pd.isnull(term['mesh_name']):
                row = term
                term_query += ' OR {disease}[MeSH Major Topic]'.format(disease = term.mesh_name.lower())
                mesh_ids.append(term.mesh_id)
                mesh_names.append(term.mesh_name)
            if not pd.isnull(term['mesh_sc_name']):
                row = term
                term_query += ' OR {disease}[Supplementary Concept]'.format(disease = term.mesh_sc_name.lower())
                mesh_ids.append(term.mesh_sc_id)
                mesh_names.append(term.mesh_sc_name)
                
        term_query = term_query.strip(' OR ')

        # skip disease entries without match mesh term/names
        if term_query == "":
            continue

        payload = {'db': 'pubmed', 'term': term_query}
        pmids = esearch_query(payload, retmax = 10000)
        row['mesh_id'] = '|'.join(mesh_ids)
        row['mesh_name'] = '|'.join(mesh_names)
        row['term_query'] = term_query
        row['n_articles'] = len(pmids)
        row['pubmed_ids'] = '|'.join(pmids)
        rows_out.append(row)
        print('{} articles for {}'.format(len(pmids), '|'.join(mesh_names)))

    make_gzip_pmid_file(rows_out, disease_pmid_file)

def download_anatomy_pmids(anatomy_file, anatomy_pmid_file):

    anatomy_df = pd.read_table(anatomy_file, sep="\t")

    rows_out = list()

    # this will take about 90 minutes
    for i, row in anatomy_df.iterrows():
        mesh_term_list = row.mesh_name.split('|')
        term_query = ""
        for mesh_term in mesh_term_list:
            term_query += ' AND {tissue}[MeSH Terms:noexp]'.format(tissue = mesh_term.lower())
        #term_query = '{tissue}[MeSH Terms:noexp]'.format(tissue = row.mesh_name.lower())
        term_query = term_query.lstrip(" AND ")
        payload = {'db': 'pubmed', 'term': term_query}
        pmids = esearch_query(payload, retmax = 5000, sleep=2)
        row['term_query'] = term_query
        row['n_articles'] = len(pmids)
        row['pubmed_ids'] = '|'.join(pmids)
        rows_out.append(row)
        log.info('{} articles for {}'.format(len(pmids), row.mesh_name))

    make_gzip_pmid_file(rows_out, anatomy_pmid_file)

def read_pmids_tsv(path, key, min_articles = 1):
    term_to_pmids = dict()
    pmids_df = pd.read_table(path, compression='gzip')
    pmids_df = pmids_df[pmids_df.n_articles >= min_articles]
    for i, row in pmids_df.iterrows():
        term = row[key]
        pmids = row.pubmed_ids.split('|')
        term_to_pmids[term] = set(pmids)
    pmids_df.drop('pubmed_ids', axis=1, inplace=True)
    return pmids_df, term_to_pmids


def score_pmid_cooccurrence(term0_to_pmids, term1_to_pmids, term0_name='term_0', term1_name='term_1', verbose=True):
    """
    Find pubmed cooccurrence between topics of two classes.
    term0_to_pmids -- a dictionary that returns the pubmed_ids for each term of class 0
    term0_to_pmids -- a dictionary that returns the pubmed_ids for each term of class 1
    """
    all_pmids0 = set.union(*term0_to_pmids.values())
    all_pmids1 = set.union(*term1_to_pmids.values())
    pmids_in_both = all_pmids0 & all_pmids1
    total_pmids = len(pmids_in_both)
    if verbose:
        print('Total articles containing a {}: {}'.format(term0_name, len(all_pmids0)))
        print('Total articles containing a {}: {}'.format(term1_name, len(all_pmids1)))
        print('Total articles containing both a {} and {}: {}'.format(term0_name, term1_name, total_pmids))

    term0_to_pmids = term0_to_pmids.copy()
    term1_to_pmids = term1_to_pmids.copy()
    for d in term0_to_pmids, term1_to_pmids:
        for key, value in list(d.items()):
            d[key] = value & pmids_in_both
            if not d[key]:
                del d[key]

    if verbose:
        print('\nAfter removing terms without any cooccurences:')
        print('+ {} {}s remain'.format(len(term0_to_pmids), term0_name))
        print('+ {} {}s remain'.format(len(term1_to_pmids), term1_name))

    rows = list()
    for term0, term1 in itertools.product(term0_to_pmids, term1_to_pmids):
        pmids0 = term0_to_pmids[term0]
        pmids1 = term1_to_pmids[term1]

        a = len(pmids0 & pmids1)
        b = len(pmids0) - a
        c = len(pmids1) - a
        d = total_pmids - len(pmids0 | pmids1)
        contingency_table = [[a, b], [c, d]]

        expected = len(pmids0) * len(pmids1) / total_pmids
        enrichment = a / expected

        oddsratio, pvalue = scipy.stats.fisher_exact(contingency_table, alternative='greater')
        rows.append([term0, term1, a, expected, enrichment, oddsratio, pvalue])

    columns = [term0_name, term1_name, 'cooccurrence', 'expected', 'enrichment', 'odds_ratio', 'p_fisher']
    df = pd.DataFrame(rows, columns=columns)

    if verbose:
        log.info('\nCooccurrence scores calculated for {} {} -- {} pairs'.format(len(df), term0_name, term1_name))
    return df

def get_anatomy_pmid_file(anatomy_file, anatomy_pmid_file, regen_pmid_files=False):

    if not os.path.isfile(config.ANATOMY_PMID_FILE) or config.REGEN_PMID_FILES:
        download_anatomy_pmids(config.UBERON_ANATOMY_FILE, config.ANATOMY_PMID_FILE)
    else:
        log.info("Skip generating Anatomy PMID file.")

def get_disease_pmid_file(disease_file, disease_xref_file, disease_mesh_xref_file, mesh_term_file,
                          disease_pmid_file, regen_pmid_files=False):

    if not os.path.isfile(config.DISEASE_PMID_FILE) or config.REGEN_PMID_FILES:
        download_disease_pmids(config.DISEASE_FILE, config.DISEASE_XREF_FILE, config.DISEASE_MESH_XREF_FILE, 
                            config.MESH_TERM_FILE, config.DISEASE_PMID_FILE)
    else:
        log.info("Skip generating Disease PMID file.")


def get_disease_anatomy_cooccurrence(anatomy_pmid_file, disease_pmid_file, disease_anatomy_output_file, p_fisher_threshold = 0.005):
 
    if os.path.isfile(disease_anatomy_output_file) or config.REGEN_PMID_FILES:
        log.info("Dataset exists:  skip generating disease anatomy cooccurrence dataset.")
        return

    uberon_df, uberon_to_pmids = read_pmids_tsv(anatomy_pmid_file, key='uberon_id')
    disease_df, disease_to_pmids = read_pmids_tsv(disease_pmid_file, key='doid')

    cooc_df = score_pmid_cooccurrence(disease_to_pmids, uberon_to_pmids, 'doid', 'uberon_id')

    cooc_df = uberon_df[['uberon_id', 'uberon_name']].drop_duplicates().merge(cooc_df)
    cooc_df = disease_df[['doid', 'name']].drop_duplicates().merge(cooc_df)

    cooc_df = cooc_df[cooc_df.p_fisher < p_fisher_threshold]

    cooc_df.to_csv(disease_anatomy_output_file, index=False, sep='\t')


if __name__ == "__main__":

    get_anatomy_pmid_file(config.UBERON_ANATOMY_FILE, config.ANATOMY_PMID_FILE, config.REGEN_PMID_FILES)

    get_disease_pmid_file(config.DISEASE_FILE, config.DISEASE_XREF_FILE, config.DISEASE_MESH_XREF_FILE, 
                          config.MESH_TERM_FILE, config.DISEASE_PMID_FILE, config.REGEN_PMID_FILES)

    get_disease_anatomy_cooccurrence(config.ANATOMY_PMID_FILE, config.DISEASE_PMID_FILE, config.DISEASE_ANATOMY_EDGE_FILE)
