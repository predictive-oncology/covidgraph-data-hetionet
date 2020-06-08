import collections
import gzip
import xml.etree.ElementTree as ET

import pandas


def parse_drugbank_xml(root):

    ns = '{http://www.drugbank.ca}'
    inchikey_template = "{ns}calculated-properties/{ns}property[{ns}kind='InChIKey']/{ns}value"
    inchi_template = "{ns}calculated-properties/{ns}property[{ns}kind='InChI']/{ns}value"

    rows = list()
    for i, drug in enumerate(root):
        row = collections.OrderedDict()
        assert drug.tag == ns + 'drug'
        row['type'] = drug.get('type')
        row['drugbank_id'] = drug.findtext(ns + "drugbank-id[@primary='true']")
        row['name'] = drug.findtext(ns + "name")
        row['description'] = drug.findtext(ns + "description")
        row['cas_number'] = drug.findtext(ns + "cas-number")
        row['groups'] = [group.text for group in
                         drug.findall("{ns}groups/{ns}group".format(ns=ns))]
        row['atc_codes'] = [code.get('code') for code in
                            drug.findall("{ns}atc-codes/{ns}atc-code".format(ns=ns))]
        row['categories'] = [x.findtext(ns + 'category') for x in
                             drug.findall("{ns}categories/{ns}category".format(ns=ns))]
        row['inchi'] = drug.findtext(inchi_template.format(ns=ns))
        row['inchikey'] = drug.findtext(inchikey_template.format(ns=ns))
        row['indication'] = drug.findtext(ns + "indication")
        row['mechanism'] = drug.findtext(ns + "mechanism-of-action")
        row['chebi_id'] = drug.findtext(
            "{ns}external-identifiers/{ns}external-identifier[{ns}resource='ChEBI']/{ns}identifier".format(ns=ns))
        row['pubchem_id'] = drug.findtext(
            "{ns}external-identifiers/{ns}external-identifier[{ns}resource='PubChem Compound']/{ns}identifier".format(ns=ns))
        row['kegg_id'] = drug.findtext(
            "{ns}external-identifiers/{ns}external-identifier[{ns}resource='KEGG Compound']/{ns}identifier".format(ns=ns))
        row['kegg_drug_id'] = drug.findtext(
            "{ns}external-identifiers/{ns}external-identifier[{ns}resource='KEGG Drug']/{ns}identifier".format(ns=ns))
        row['chemspider_id'] = drug.findtext(
            "{ns}external-identifiers/{ns}external-identifier[{ns}resource='ChemSpider']/{ns}identifier".format(ns=ns))

        # Add drug aliases
        aliases = {
            elem.text for elem in
            drug.findall("{ns}international-brands/{ns}international-brand".format(ns=ns)) +
            drug.findall("{ns}synonyms/{ns}synonym[@language='English']".format(ns=ns)) +
            drug.findall("{ns}international-brands/{ns}international-brand".format(ns=ns)) +
            drug.findall("{ns}products/{ns}product/{ns}name".format(ns=ns))

        }
        aliases.add(row['name'])
        row['aliases'] = sorted(aliases)
        row['license'] = 'het CC0 1.0'
        row['source'] = 'DrugBank'
        row['source_url'] = "https://www.drugbank.ca/drugs/{0}".format(row['drugbank_id'])

        rows.append(row)

    return rows


def collapse_list_values(row):
    for key, value in row.items():
        if isinstance(value, list):
            row[key] = '|'.join(value)
    return row


def load_drugbank_vocab_file(drugbank_vocabulary_file):

    df = pandas.read_table(drugbank_vocabulary_file, sep=",")
    df.columns = ["drugbank_id", "accession_numbers", "name", "cas_number", "unii", "synonyms", "inchi_key"]
    df["source"] = "DrugBank"
    df["source_url"] = "https://www.drugbank.ca/drugs/" + df["drugbank_id"]

    return df


def load_drugbank_file(drugbank_vocabulary_file, drugbank_xml_file, compound_output_file):

    with gzip.open(drugbank_xml_file) as xml_file:
        tree = ET.parse(xml_file)
    root = tree.getroot()

    rows = parse_drugbank_xml(root)
    rows = list(map(collapse_list_values, rows))

    columns = ['drugbank_id', 'name', 'type', 'groups', 'cas_number', 'atc_codes', 'categories', 'inchikey', 'inchi', 'description', 'indication', 'mechanism',
               'chebi_id', 'pubchem_id', 'kegg_id', 'kegg_drug_id', 'chemspider_id', 'license', 'source', 'source_url']
    drugbank_df = pandas.DataFrame.from_dict(rows)[columns]
    drugbank_df["source_id"] = drugbank_df.drugbank_id
    drugbank_df.inchikey = drugbank_df.inchikey.str.replace('InChIKey=', '')
    drugbank_df.inchi = drugbank_df.inchi.str.replace('InChI=', '')

    drugbank_df.to_csv(compound_output_file, sep="\t")

    return drugbank_df
