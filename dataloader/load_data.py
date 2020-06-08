
import concurrent
import functools
import logging
import multiprocessing
import os
from typing import Any, Dict, List

import pandas
from Configs import getConfig
from pebble import ProcessPool
from py2neo import Node

from compound_parser import load_compounds
from disease_ontology_parser import load_disease_file
from json2graphio import Json2graphio
from compound_parser import load_compounds
from ttd_target_parser import load_target_file
from disease_ontology_parser import load_disease_file
from uberon_parser import load_uberon_anatomy_file
from mesh_desc_parser import load_mesh_descriptor_files
from ttd_target_parser import load_target_file

config = getConfig()
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(getattr(logging, config.LOG_LEVEL))
graph = config.get_graph()


class Entity(object):

    _raw_data_json = None
    _raw_data_csv_row = None
    properties = None

    def __init__(self,
                 row: pandas.Series,
                 label=None,
                 name=None,
                 parser=None):

        self._raw_data_csv_row = row
        self.properties = {}
        self.label = label
        self.name = name
        parser(self)

    def to_dict(self):

        dic = self.properties
        return dic


class EntityParser(object):

    def __init__(self, entity: Entity):

        self.entity = entity
        self.parse_entity_properties()

    def parse_entity_properties(self):

        for prop_name in config.ENTITY_PROPERTY_COLUMNS[self.entity.name]:

            prop_val = self.entity._raw_data_csv_row[prop_name]

            if not pandas.isna(prop_val):
                self.entity.properties[prop_name] = prop_val


class Compound(Entity):

    def __init__(self, row: pandas.Series):

        label = "Compound"
        name = "Compound"
        super(Compound, self).__init__(row, label=label, name=name, parser=CompoundParser)


class CompoundParser(EntityParser):

    def __init__(self, compound: Compound):

        super(CompoundParser, self).__init__(compound)


class Target(Entity):

    def __init__(self, row: pandas.Series):

        label = "Target"
        super(Target, self).__init__(row, label=label, name=label, parser=TargetParser)


class TargetParser(EntityParser):

    def __init__(self, target: Target):

        super(TargetParser, self).__init__(target)


class Anatomy(Entity):

    def __init__(self, row: pandas.Series):

        label = "Anatomy"
        super(Anatomy, self).__init__(row, label=label, name=label, parser=AnatomyParser)


class AnatomyParser(EntityParser):

    def __init__(self, anatomy: Anatomy):

        super(AnatomyParser, self).__init__(anatomy)


class TargetCompoundMap():

    def __init__(self, row: pandas.Series):

        self._raw_data_csv_row = row

        self.parent_label = "Compound"
        self.parent_node = None
        self.parent_json_data = {}

        self.child_label = "Target"
        self.child_node = None
        self.child_json_data = {}

        self.properties = {}
        self.custom_relationship_id = None

        self.is_relationship = True

        TargetCompoundMapParser(self)

    def get_parent_node(self, data_loader):

        node = data_loader._adjust_label_name(Node(self.parent_label))

        for key, value in self.parent_json_data.items():
            node[key] = value

        label_name_adjusted = node.__primarylabel__
        node = data_loader._generate_id_attr(node, self.parent_json_data)

        return node

    def get_child_node(self, data_loader):

        node = data_loader._adjust_label_name(Node(self.child_label))

        for key, value in self.child_json_data.items():
            node[key] = value

        label_name_adjusted = node.__primarylabel__
        node = data_loader._generate_id_attr(node, self.child_json_data)

        return node


class TargetCompoundMapParser():

    def __init__(self, target_compound_map: TargetCompoundMap):

        self.entity = target_compound_map

        drugbank_id = self.entity._raw_data_csv_row["drugbank_id"]
        ttd_drug_id = self.entity._raw_data_csv_row["ttd_drug_id"]
        if isinstance(drugbank_id, float):
            drugbank_id = ""
        if isinstance(ttd_drug_id, float):
            ttd_drug_id = ""

        self.entity.parent_node = Node(self.entity.parent_label)
        self.entity.parent_json_data = {"drugbank_id": drugbank_id,
                                        "ttd_id": ttd_drug_id}

        child_id = self.entity._raw_data_csv_row["ttd_id"]
        self.entity.child_node = Node(self.entity.child_label)
        self.entity.child_json_data = {"ttd_id": child_id}

        for prop_name in ['moa', 'activity', 'reference']:

            prop_val = self.entity._raw_data_csv_row[prop_name]
            if not pandas.isna(prop_val):
                self.entity.properties[prop_name] = prop_val

        moa = ""
        if "moa" in self.entity.properties:
            moa = self.entity.properties['moa']

        self.entity.custom_relationship_id = (
            frozenset(self.entity.parent_node.labels),
            frozenset(self.entity.child_node.labels),
            moa
        )


class DiseaseAnatomyMap():

    def __init__(self, row: pandas.Series):

        self._raw_data_csv_row = row

        self.parent_label = "Disease"
        self.parent_node = None
        self.parent_json_data = {}

        self.child_label = "Anatomy"
        self.child_node = None
        self.child_json_data = {}

        self.properties = {}
        self.custom_relationship_id = None

        self.is_relationship = True

        DiseaseAnatomyMapParser(self)

    def get_parent_node(self, data_loader):

        node = data_loader._adjust_label_name(Node(self.parent_label))

        for key, value in self.parent_json_data.items():
            node[key] = value

        label_name_adjusted = node.__primarylabel__
        node = data_loader._generate_id_attr(node, self.parent_json_data)

        return node

    def get_child_node(self, data_loader):

        node = data_loader._adjust_label_name(Node(self.child_label))

        for key, value in self.child_json_data.items():
            node[key] = value

        label_name_adjusted = node.__primarylabel__
        node = data_loader._generate_id_attr(node, self.child_json_data)

        return node


class DiseaseAnatomyMapParser():

    def __init__(self, disease_anatomy_map: DiseaseAnatomyMap):

        self.entity = disease_anatomy_map

        disease_id = self.entity._raw_data_csv_row["doid"]
        anatomy_id = self.entity._raw_data_csv_row["uberon_id"]
        if isinstance(disease_id, float):
            disease_id = ""
        if isinstance(anatomy_id, float):
            anatomy_id = ""

        self.entity.parent_node = Node(self.entity.parent_label)
        self.entity.parent_json_data = {"doid": disease_id}

        child_id = self.entity._raw_data_csv_row["uberon_id"]
        self.entity.child_node = Node(self.entity.child_label)
        self.entity.child_json_data = {"uberon_id": anatomy_id}

        for prop_name in ['cooccurrence', 'expected', 'p_fisher']:

            prop_val = self.entity._raw_data_csv_row[prop_name]
            if not pandas.isna(prop_val):
                self.entity.properties[prop_name] = prop_val

        self.entity.custom_relationship_id = (
            frozenset(self.entity.parent_node.labels),
            frozenset(self.entity.child_node.labels)
        )


class DiseaseAssociatesGene:

    __slots__ = ('parent_node', 'parent_json_data', 'child_node', 'child_json_data', 'properties')

    def __init__(self, data_row: pandas.Series):
        self._parse_attributes_from(data_row)

    @staticmethod
    def _parse_properties(data_row: pandas.Series, property_names: List[str]):
        rel_properties = {}
        for prop_name in property_names:
            prop_val = data_row[prop_name]
            if not pandas.isna(prop_val):
                rel_properties[prop_name] = prop_val

        return rel_properties

    @property
    def parent_label(self) -> str:
        return "Disease"

    @property
    def child_label(self) -> str:
        return "Gene"

    @property
    def is_relationship(self) -> bool:
        return True

    @property
    def parent_id_column(self) -> str:
        return "doid_code"

    @property
    def parent_id_field(self) -> str:
        return "doid"

    @property
    def child_id_column(self) -> str:
        return "gene"

    @property
    def child_id_field(self) -> str:
        return "GeneID"

    @property
    def property_names(self) -> List[str]:
        return ['locus', 'high_confidence', 'primary', 'status']

    @property
    def custom_relationship_id(self) -> str:
        return "DISEASE:GENE"

    def get_parent_node(self, data_loader: Json2graphio) -> Json2graphio:
        return self._get_node(data_loader, label=self.parent_label, json_data=self.parent_json_data)

    def get_child_node(self, data_loader: Json2graphio) -> Json2graphio:
        return self._get_node(data_loader, label=self.child_label, json_data=self.child_json_data)

    def _get_node(self, data_loader: Json2graphio, label: str, json_data: Dict[str, Any]) -> Json2graphio:
        node = data_loader._adjust_label_name(Node(label))

        for key, value in json_data.items():
            node[key] = value

        return data_loader._generate_id_attr(node, json_data)

    def _parse_attributes_from(self, data_row: pandas.Series) -> None:
        self.parent_node = Node(self.parent_label)
        parent_id = data_row[self.parent_id_column]
        self.parent_json_data = {self.parent_id_field: parent_id}

        self.child_node = Node(self.child_label)
        child_id = str(data_row[self.child_id_column])
        self.child_json_data = {self.child_id_field: child_id}

        self.properties = self._parse_properties(data_row, self.property_names)


class Disease(Entity):

    def __init__(self, row: pandas.Series):

        label = "Disease"
        self.parents = []
        super(Disease, self).__init__(row, label=label, name=label, parser=DiseaseParser)

    def to_dict(self):

        dic = super(Disease, self).to_dict()
        dic['Disease'] = self.parents

        return dic


class DiseaseParser(EntityParser):

    def __init__(self, disease: Disease):

        super(DiseaseParser, self).__init__(disease)

        self.parse_disease_parents()

    def parse_disease_parents(self):

        parents = []
        try:
            parents = self.entity._raw_data_csv_row['parents']
            parents = parents.split(",")
            parents = [{'doid': p} for p in parents]

            if len(parents) > 0:
                self.entity.parents = parents
        except Exception as error:
            pass

def _custom_relation_name_generator(parent_node, child_node, relation_props):

    rel_key = "{0}:{1}".format(parent_node.__primarylabel__.upper(), child_node.__primarylabel__.upper())

    if rel_key in config.JSON2GRAPH_RELTYPE:

        rel_val = config.JSON2GRAPH_RELTYPE[rel_key]

        if isinstance(rel_val, str):
            return rel_val
        else:

            prop_key = rel_val[0]
            rel_name_lookup = rel_val[1]

            if not prop_key in relation_props:
                log.error("Property {0} not found for {1} edge".format(prop_key, rel_key))
            else:
                # get property value
                prop_val = relation_props[prop_key]

                # lookup relation name using property value
                if prop_val in rel_name_lookup:
                    return rel_name_lookup[prop_val]

            try:
                return rel_name_lookup["default"]
            except:
                pass

    return None


class Dataloader(object):
    def __init__(self, dataset_file, entity_name, from_row=None, to_row=None, worker_name: str = None,):
        self.name = worker_name
        self.data = pandas.read_csv(dataset_file, sep="\t")[from_row:to_row]
        self.entity_name = entity_name

        self.entity_lookup = {
            "Compound": Compound,
            "TargetCompoundMap": TargetCompoundMap,
            "Target": Target,
            "Disease": Disease,
            "Anatomy": Anatomy,
            "DiseaseAnatomyMap": DiseaseAnatomyMap,
            "DiseaseAssociatesGene": DiseaseAssociatesGene
        }

        self.entity_label_lookup = {
            "Compound": "Compound",
            "TargetCompoundMap": "Target",
            "Target": "Target",
            "Disease": "Disease",
            "Anatomy": "Anatomy",
            "DiseaseAnatomyMap": "Disease",
            "DiseaseAssociatesGene": "ASSOCIATES_DaG"
        }

        self.entity = self.entity_lookup[self.entity_name]
        self.entity_label = self.entity_label_lookup[self.entity_name]

        self.data = self.data.rename(
            columns=config.METADATA_FILE_COLUMN_OVERRIDE[self.entity_name], errors="raise"
        )
        self._build_loader()

    def parse(self):
        nodes = []
        node_total_count = len(self.data)

        node_count = 0
        for index, row in self.data.iterrows():
            nodes.append(self.entity(row))
            if len(nodes) == config.BATCH_SIZE:
                log.info(
                    "{}Load next {} {}.".format(
                        self.name + ": " if self.name else "", len(nodes), self.entity_name.lower()
                    )
                )
                self.load(nodes)
                node_count += len(nodes)
                del nodes
                nodes = []
                log.info(
                    "{}Loaded {} from {} {}s.".format(
                        self.name + ": " if self.name else "",
                        node_count,
                        node_total_count,
                        self.entity_name.lower()
                    )
                )
        self.load(nodes)

    def load(self, nodes):

        for index, node in enumerate(nodes):
            if hasattr(node, "is_relationship"):
                parent_node = node.get_parent_node(self.loader)
                child_node = node.get_child_node(self.loader)

                self.loader._create_relation(parent_node, child_node, node.properties, node.custom_relationship_id)
            else:
                self.loader.load_json(node.to_dict(), self.entity_label)
        try:
            if db_loading_lock is not None:
                db_loading_lock.acquire()
                log.info(
                    "{}Acquired DB loading lock.".format(
                        self.name + ": " if self.name else ""
                    )
                )
        except NameError:
            # we are in singlethreaded mode. no lock set
            pass
        try:
            self.loader.create_indexes(graph)
            self.loader.merge(graph)
        finally:
            try:
                if db_loading_lock is not None:
                    log.info(
                        "{}Release DB loading lock.".format(
                            self.name + ": " if self.name else ""
                        )
                    )
                    db_loading_lock.release()
            except NameError:
                # we are in singlethreaded mode. no lock set
                pass


    def _build_loader(self):
        c = Json2graphio()
        # c.config_dict_label_override = config.JSON2GRAPH_LABELOVERRIDE
        c.config_func_custom_relation_name_generator = _custom_relation_name_generator
        c.config_dict_primarykey_generated_hashed_attrs_by_label = (
            config.JSON2GRAPH_GENERATED_HASH_IDS
        )
        c.config_list_skip_collection_hubs = "all"
        c.config_dict_concat_list_attr = config.JSON2GRAPH_CONCAT_LIST_ATTR
        c.config_str_collection_anchor_label = config.JSON2GRAPH_COLLECTION_NODE_LABEL
        c.config_list_collection_anchor_extra_labels = (
            config.JSON2GRAPH_COLLECTION_EXTRA_LABELS
        )
        c.config_graphio_batch_size = config.COMMIT_INTERVAL
        #c.config_dict_primarykey_attr_by_label = config.JSON2GRAPH_ID_ATTR

        primarykey_generated_attr_name = "{0}{1}".format(
            self.entity_label.lower(), config.JSON2GRAPH_GENERATED_HASH_ID_ATTR_NAME)
        c.config_str_primarykey_generated_attr_name = (
            config.JSON2GRAPH_GENERATED_HASH_ID_ATTR_NAME
        )
        c.config_bool_capitalize_labels = False
        c.config_dict_label_override = config.JSON2GRAPH_LABEL_OVERRIDE
        # c.config_func_node_pre_modifier = DataTransformer.renameLabels
        # c.config_func_node_post_modifier = DataTransformer.addExtraLabels
        # c.config_dict_property_name_override = config.JSON2GRAPH_PROPOVERRIDE
        self.loader = c

    # Todo: Make Worker class to function and create pool
    # https://stackoverflow.com/questions/20886565/using-multiprocessing-process-with-a-maximum-number-of-simultaneous-processes


def worker_task(dataset_file, entity_label, from_row: int, to_row: int, worker_name: str,):

    log.info("Start {} -- row {} to row {}".format(worker_name, from_row, to_row))
    # l = 1 / 0
    dataloader = Dataloader(
        dataset_file, entity_label, from_row=from_row, to_row=to_row, worker_name=worker_name,
    )
    dataloader.parse()


def worker_task_init(l):
    global db_loading_lock
    db_loading_lock = l


def worker_task_done(task_name, pool, other_futures, dataset_file, from_row, to_row, future):
    try:
        result = future.result()
    except concurrent.futures.CancelledError:
        # canceled by god or anyone
        log.info("{} cancelled".format(task_name))
        return
    except Exception as error:
        if config.CANCEL_WHOLE_IMPORT_IF_A_WORKER_FAILS:
            log.warning(
                "{} failed. Cancel all tasks and stop workers...".format(task_name)
            )
            pool.close()

            for fut in other_futures:
                fut.cancel()
            future.cancel()
        log.info("{} failed".format(task_name))
        log.exception("[{}] Function raised {}".format(task_name, error))
        log.info(
            "Exception happened in {} -> row range {} - {}".format(
                dataset_file, from_row, to_row
            )
        )
        if config.CANCEL_WHOLE_IMPORT_IF_A_WORKER_FAILS:
            pool.stop()
        global exit_code
        exit_code = 1
        raise error
    log.info("{} finished".format(task_name))
    return


def load_data_mp(worker_count: int, rows_per_worker=None):

    global exit_code
    exit_code = 0

    input_files = [config.COMPOUND_FILE, config.TTD_TARGET_FILE,
                   config.TTD_COMPOUND_TARGET_MAP_FILE, config.DISEASE_FILE, config.DISEASE_ASSOCIATES_GENE_FILE,
                   config.UBERON_ANATOMY_FILE, config.DISEASE_ANATOMY_EDGE_FILE]

    entity_names = ["Compound", "Target", "TargetCompoundMap",
                    "Disease", "DiseaseAssociatesGene", "Anatomy", "DiseaseAnatomyMap"]

    for dataset_file, entity_name in zip(input_files, entity_names):

        log.info("Loading {0} nodes".format(entity_name))
        row_count_total = len(pandas.read_csv(dataset_file, sep="\t").dropna(how="all"))
        log.info("Loading {0} {1} nodes ...".format(entity_name, row_count_total))

        if rows_per_worker is None:
            # just distribute all rows to workers. all workers will run simulationsly
            rows_per_worker = int(row_count_total / worker_count)
            leftover_rows = row_count_total % worker_count
            worker_instances_count = worker_count
        else:
            # we create a queue of workers, only <worker_count> will run simulationsly
            worker_instances_count = int(row_count_total / rows_per_worker) or 1
            leftover_rows = row_count_total % rows_per_worker

        log.info("Worker instance count = {}".format(worker_instances_count))
        log.info("Leftover row count = {}".format(leftover_rows))

        lock = multiprocessing.Lock()
        rows_distributed = 0
        futures = []
        with ProcessPool(
            max_workers=worker_count,
            max_tasks=1,
            initializer=worker_task_init,
            initargs=(lock,),
        ) as pool:
            for worker_index in range(0, worker_instances_count):
                from_row = rows_distributed
                rows_distributed += rows_per_worker
                if worker_index == worker_instances_count - 1:
                    # last worker gets the leftofter rows
                    rows_distributed += leftover_rows
                worker_task_name = "WORKER_TASK_{}".format(worker_index)
                log.info("Add worker task '{}' to schedule to process rows: {} - {}".format(worker_task_name,
                                                                                            from_row, rows_distributed - 1))
                future = pool.schedule(
                    worker_task,
                    args=(
                        dataset_file,
                        entity_name,
                        from_row,
                        rows_distributed,
                        worker_task_name,
                    ),
                )

                future.add_done_callback(
                    functools.partial(
                        worker_task_done,
                        worker_task_name,
                        pool,
                        futures,
                        dataset_file,
                        from_row,
                        rows_distributed,
                    )
                )
                futures.append(future)
                rows_distributed += 1
    exit(exit_code)


# Simple singleprocessed loading
def load_data():

    if not os.path.exists(config.COMPOUND_FILE):
        load_compounds(config.DRUGBANK_VOCABULARY_FILE, config.DRUGBANK_XML_FILE, config.TTD_DRUG_DOWNLOAD_FILE, config.TTD_DRUG_XREF_DOWNLOAD_FILE,
                       config.DRUGBANK_COMPOUND_FILE, config.TTD_COMPOUND_FILE, config.COMPOUND_FILE)

    entity_label = "Compound"
    dataloader = Dataloader(config.COMPOUND_FILE, entity_label)
    dataloader.parse()


if __name__ == "__main__":

    load_compounds(config.DRUGBANK_VOCABULARY_FILE, config.DRUGBANK_XML_FILE, config.TTD_DRUG_DOWNLOAD_FILE, config.TTD_DRUG_XREF_DOWNLOAD_FILE,
                   config.DRUGBANK_COMPOUND_FILE, config.TTD_COMPOUND_FILE, config.COMPOUND_FILE)
    load_target_file(config.TTD_TARGET_FILE, config.TTD_TARGET_FILE)
    load_disease_file(config.DISEASE_OBO_FILE, config.DISEASE_FILE, config.DISEASE_XREF_FILE)

    load_mesh_descriptor_files()
    load_uberon_anatomy_file(config.UBERON_BASIC_OBO_FILE, config.UBERON_TERM_FILE, config.UBERON_XREF_FILE, config.UBERON_SUBSET_FILE,
                             config.HUMAN_CONSTRAINTS_FILE, config.MESH_TERM_FILE, config.ANATOMY_OUTPUT_FILE)

    # with CodeTimer(unit="s"):
    load_data_mp(config.NO_OF_PROCESSES, config.BATCH_SIZE)
    # load_data_mp(2, 1)
    # load_data()
