import hashlib
import json
import uuid

from graphio import NodeSet, RelationshipSet
from py2neo import Graph, Node

# i know this is a mess. the great refactoring is coming...


class Json2graphio(object):
    config_bool_capitalize_labels = False
    # Override generated label names (which are based on json attr names)
    # e.g. config_dict_label_override = {"my_auto_label":"MyAutoLabel"}
    # optional you can attach extra attributes to the node
    # e.g. config_dict_label_override = {"my_auto_label_number4":{"MyAutoLabel":{"number":"4"}}
    config_dict_label_override = None
    config_dict_reltype_override = None
    config_list_drop_reltypes = None
    # Override property names for certain nodes
    # e.g. config_dict_property_name_override = {"Person":{"sons":"son","daughters":daughter}}
    config_dict_property_name_override = None
    config_list_default_primarykeys = ["id", "_id"]
    config_dict_primarykey_attr_by_label = None
    config_dict_primarykey_generated_hashed_attrs_by_label = None
    config_str_primarykey_generated_attr_name = "_id"
    # Collection hubs node label name. 'LIST_MEMBER_LABEL' can be used as placeholders var. e.g. "{LIST_MEMBER_LABEL}_Collection"
    config_str_collection_anchor_label = "{LIST_MEMBER_LABEL}Collection"
    config_list_collection_anchor_extra_labels = ["CollectionHub"]
    # names of collections hubs that should be converted to multiple direct relations e.g. ["MyCollection","OtherCollection"]
    # Set to "all" to disable collection hubs
    config_list_skip_collection_hubs = None
    # If set to true, all collections hubs get a second label, named after the list member nodes
    config_str_collection_anchor_attach_list_members_label = False
    config_str_collection_relation_postfix = "_COLLECTION"
    config_bool_collection_anchor_only_when_len_min_2 = False
    config_func_custom_relation_name_generator = None
    config_func_label_name_generator_func = None
    config_dict_concat_list_attr = None
    config_func_node_post_modifier = None
    config_func_node_pre_modifier = None
    config_graphio_batch_size = 10000
    config_dict_create_merge_depending_scheme = None
    config_dict_property_to_extra_node = None
    # config_dict_interfold_json_attr
    # Reduce a chain of json attr into a node instead of multiple nodes
    # or in other words: merge child objects into its parent
    # {json-object-name:{json-object-attr:{"attrs":[json-sub-attrs],"combine_attr_names":False|True}
    # or
    # {json-object-name:{json-object-attr:None}}
    # e.g. source: {person:{"name":"Rita",family":{direct:["{"person":"husband"}","child"],extended:["brother-in-law","sister-in-law"]}}}
    # will result in (:person)->(:family)->(:direct|extended)->(:person)
    # config_dict_interfold_json_attr = {"person":{"family":{"attrs":["direct","extended"]}
    # will result in (:person)->(:family_direct|family_extended)->(:person)
    config_dict_interfold_json_attr = None
    # Pivot json attribut name to relation type name and define alternative label for content
    # e.g. {"inventor":"Person"}
    config_dict_json_attr_to_reltype_instead_of_label = None
    relationshipSets = None
    nodeSets = None

    def __init__(self, data=None):
        self.config_dict_label_override = {}
        self.config_dict_reltype_override = {}
        self.config_dict_property_name_override = {}
        self.config_dict_primarykey_generated_hashed_attrs_by_label = {}
        self.config_dict_primarykey_attr_by_label = {}
        self.config_dict_concat_list_attr = {}
        self.relationshipSets = {}
        self.nodeSets = {}
        self.config_list_skip_collection_hubs = []
        self.config_dict_create_merge_depending_scheme = {"create": [], "merge": []}
        self.config_dict_json_attr_to_reltype_instead_of_label = {}
        self.config_list_drop_reltypes = []
        self._blocked_reltypes = []

    def load_json(self, data, parent_label_name=None):
        if isinstance(data, str):
            j = json.loads(data)
        else:
            j = data
        if not isinstance(j, dict) and not isinstance(j, list):
            raise ValueError(
                "Expected json string, dict or list. got {}".format(type(j).__name__)
            )
        self._jsondict2subgraph(parent_label_name, j)

    def merge(self, graph: Graph):
        for nodes in self.nodeSets.values():
            nodes.merge(graph, batch_size=self.config_graphio_batch_size)
        for rels in self.relationshipSets.values():
            rels.merge(graph, batch_size=self.config_graphio_batch_size)

    def create(self, graph: Graph):
        for nodes in self.nodeSets.values():
            nodes.create(graph, batch_size=self.config_graphio_batch_size)
        for rels in self.relationshipSets.values():
            rels.create(graph, batch_size=self.config_graphio_batch_size)

    def create_merge_depending(self, graph, default="create"):
        # ToDo: make this switch tree more elegant. this is ugly
        for nodes in self.nodeSets.values():
            if nodes.labels in self.config_dict_create_merge_depending_scheme["create"]:
                nodes.create(graph, batch_size=self.config_graphio_batch_size)
            elif (
                nodes.labels in self.config_dict_create_merge_depending_scheme["merge"]
            ):
                nodes.merge(graph, batch_size=self.config_graphio_batch_size)
            else:
                if default == "create":
                    nodes.create(graph, batch_size=self.config_graphio_batch_size)
                else:
                    nodes.merge(graph, batch_size=self.config_graphio_batch_size)

        for rels in self.relationshipSets.values():
            if (
                rels.rel_type
                in self.config_dict_create_merge_depending_scheme["create"]
            ):
                rels.create(graph, batch_size=self.config_graphio_batch_size)
            elif (
                rels.rel_type in self.config_dict_create_merge_depending_scheme["merge"]
            ):
                rels.merge(graph, batch_size=self.config_graphio_batch_size)
            else:
                if default == "create":
                    rels.create(graph, batch_size=self.config_graphio_batch_size)
                else:
                    rels.merge(graph, batch_size=self.config_graphio_batch_size)

    def create_indexes(self, graph: Graph):
        for rels in self.relationshipSets.values():
            rels.create_index(graph)

    def _generate_id_attr(self, node, json_data):
        if (
            node.__primarylabel__
            in self.config_dict_primarykey_generated_hashed_attrs_by_label
        ):
            hash_attrs = self.config_dict_primarykey_generated_hashed_attrs_by_label[
                node.__primarylabel__
            ]

            if hash_attrs == "AllInnerContent":
                node[self.config_str_primarykey_generated_attr_name] = hashlib.md5(
                    json.dumps(json_data).encode()
                ).hexdigest()
            elif hash_attrs == "AllAttributes":
                node[self.config_str_primarykey_generated_attr_name] = hashlib.md5(
                    json.dumps(dict(node)).encode()
                ).hexdigest()
            elif isinstance(hash_attrs, list):
                # generate a hash based on specific node properties
                id_hash = hashlib.md5()

                node_dict = dict(node)
                for key in sorted(node_dict.keys()):
                    if key in hash_attrs:
                        id_hash.update(node_dict[key].encode())
                node[
                    self.config_str_primarykey_generated_attr_name
                ] = id_hash.hexdigest()
            if hash_attrs is None:
                # generate a random hash
                node[self.config_str_primarykey_generated_attr_name] = uuid.uuid4().hex

            node.__primarykey__ = self.config_str_primarykey_generated_attr_name
        return node

    def _adjust_label_name(self, node: Node) -> str:
        label_name = list(node.labels)[0]
        node.clear_labels()
        if callable(self.config_func_label_name_generator_func):
            custom_name = self.config_func_label_name_generator_func(label_name)
            if custom_name is not None:
                return custom_name
        label_name_adjusted = label_name
        if label_name in self.config_dict_json_attr_to_reltype_instead_of_label:
            label_name_adjusted = self.config_dict_json_attr_to_reltype_instead_of_label[
                label_name
            ]
        if label_name in self.config_dict_label_override:
            label_name_override_config = self.config_dict_label_override[label_name]
            if isinstance(label_name_override_config, str):
                label_name_adjusted = label_name_override_config
            elif isinstance(label_name_override_config, dict):
                label_name_adjusted = list(label_name_override_config.keys())[0]
                # add extra props as configured by caller
                extra_props = list(label_name_override_config.values())[0]
                for extra_prop, extra_val in extra_props.items():
                    node[extra_prop] = extra_val
        label_name_adjusted = (
            label_name_adjusted.capitalize()
            if self.config_bool_capitalize_labels
            else label_name_adjusted
        )
        node.add_label(label_name_adjusted)
        node.__primarylabel__ = label_name_adjusted
        return node

    def _is_basic_type(self, val):
        if isinstance(val, (str, int, float, bool)):
            return True
        else:
            return False

    def _is_empty(self, val):
        if not val:
            return True
        if isinstance(val, str) and val.upper() in ["", "NULL", "NONE"]:
            return True
        return False

    def _create_relation(self, parent_node: Node, child_node: Node, relation_props={}, relationshipset_identifier=None):

        if parent_node is None or child_node is None:
            return None
        # labels = ":".join(parent_node.labels) + "|" + ":".join(child_node.labels)

        if relationshipset_identifier == None:
            relationshipset_identifier = (
                frozenset(parent_node.labels),
                frozenset(child_node.labels),
            )

        if (
            hasattr(parent_node, "override_reltype")
            and child_node.__primarylabel__ in parent_node.override_reltype
        ):
            relationshipset_identifier = (
                frozenset(parent_node.labels),
                frozenset(child_node.labels),
                frozenset(parent_node.override_reltype[child_node.__primarylabel__]),
            )

        # Create new relationshipset if necessary
        if not relationshipset_identifier in self.relationshipSets:
            rel_name = None
            if callable(self.config_func_custom_relation_name_generator):
                rel_name = self.config_func_custom_relation_name_generator(
                    parent_node, child_node, relation_props
                )
            if rel_name is None:
                child_node_name = child_node.__primarylabel__.upper()
                parent_node_name = parent_node.__primarylabel__.upper()
                rel_name = "{}_HAS_{}".format(parent_node_name, child_node_name,)
                if hasattr(parent_node, "override_reltype"):
                    if child_node.__primarylabel__ in parent_node.override_reltype:
                        rel_name = parent_node.override_reltype[
                            child_node.__primarylabel__
                        ].upper()

                if rel_name in self.config_dict_reltype_override:
                    rel_name = self.config_dict_reltype_override[rel_name]
            if rel_name in self.config_list_drop_reltypes:
                self._blocked_reltypes.append(relationshipset_identifier)
            else:
                self.relationshipSets[relationshipset_identifier] = RelationshipSet(
                    rel_type=rel_name,
                    start_node_labels=frozenset(parent_node.labels),
                    end_node_labels=frozenset(child_node.labels),
                    start_node_properties=self._get_merge_keys(parent_node),
                    end_node_properties=self._get_merge_keys(child_node),
                )
        # add relationship to set if not blocked by caller config
        if not relationshipset_identifier in self._blocked_reltypes:
            self.relationshipSets[relationshipset_identifier].add_relationship(
                start_node_properties={
                    key: val
                    for key, val in dict(parent_node).items()
                    if key in self._get_merge_keys(parent_node)
                },
                end_node_properties={
                    key: val
                    for key, val in dict(child_node).items()
                    if key in self._get_merge_keys(child_node)
                },
                properties=relation_props,
            )

    def _add_node(self, node):
        # create nodeSet if necessary
        labels = frozenset(node.labels)
        if not labels in self.nodeSets:
            # get primary keys
            self.nodeSets[labels] = NodeSet(
                list(labels), merge_keys=self._get_merge_keys(node)
            )
        # add node to nodeset
        self.nodeSets[labels].nodes.append(node)

    def _get_merge_keys(self, node):
        labels = node.__primarylabel__
        if hasattr(node, "_is_collectionhub"):
            return ["id"]
        if labels in self.config_dict_primarykey_attr_by_label:
            merge_keys = self.config_dict_primarykey_attr_by_label[labels]
        elif labels in self.config_dict_primarykey_generated_hashed_attrs_by_label:
            merge_keys = self.config_str_primarykey_generated_attr_name
        else:
            merge_keys = list(dict(node).keys())
        if not isinstance(merge_keys, list):
            merge_keys = [merge_keys]
        return merge_keys

    def _adjust_property_name(self, label, property_name):
        if label in self.config_dict_property_name_override:
            if property_name in self.config_dict_property_name_override[label]:
                return self.config_dict_property_name_override[label][property_name]
        return property_name

    def _get_hub_node_label_name(self, member_label_name):
        label = self.config_str_collection_anchor_label.format(
            LIST_MEMBER_LABEL=member_label_name
        )
        if label in self.config_dict_label_override:
            label = self.config_dict_label_override[label]
        return label

    def _create_collection_hub_node(self, member_label_name, hub_id):
        hub_node_label = self._get_hub_node_label_name(member_label_name)
        hub_node = Node(hub_node_label, id=hub_id)
        hub_node._is_collectionhub = True
        hub_node.__primarylabel__ = hub_node_label
        hub_node.__primarykey__ = "id"
        for lbl in self.config_list_collection_anchor_extra_labels:
            hub_node.add_label(lbl)
        if self.config_str_collection_anchor_attach_list_members_label:
            hub_node.add_label(member_label_name)
        return hub_node

    def _fold_json_attrs(self, key, val):

        if (
            self.config_dict_interfold_json_attr is not None
            and key in self.config_dict_interfold_json_attr
        ):

            folding_rules = self.config_dict_interfold_json_attr[key]
            for folding_attr, folding_param in folding_rules.items():

                # default params
                # transfer all attrs
                child_content = val[folding_attr]
                # do not combine parent and child attrs
                combine_names = True
                if folding_param is not None:
                    if "combine_attr_names" in folding_param:
                        combine_names = folding_param["combine_attr_names"]
                    if "attrs" in folding_param:
                        child_content = {
                            key: val[key]
                            for key in val.keys()
                            if key in folding_attr["attrs"]
                        }
                for (
                    folding_child_attr_key,
                    folding_child_attr_val,
                ) in child_content.items():
                    if combine_names:
                        new_attr_name = "{}_{}".format(
                            folding_attr, folding_child_attr_key
                        )
                    else:
                        new_attr_name = folding_child_attr_key
                    val[new_attr_name] = folding_child_attr_val
                del val[folding_attr]
            return val

    def _jsondict2subgraph(self, label_name: str, json_data, parent_node=None) -> Node:
        """[summary]

        Arguments:
            self {[type]} -- [description]
            Node {[type]} -- [description]
        Returns:
            set(Subgraph, Node) -- The generated subgraph and the top anchor node from the subgraph
        """
        if self._is_empty(json_data):
            return None
        if label_name is not None:
            node = self._adjust_label_name(Node(label_name))
            label_name_adjusted = node.__primarylabel__

            if label_name_adjusted in self.config_dict_primarykey_attr_by_label:
                node.__primarykey__ = self.config_dict_primarykey_attr_by_label[
                    label_name_adjusted
                ]
            if callable(self.config_func_node_pre_modifier):
                node = self.config_func_node_pre_modifier(node)
        elif label_name is None and self._is_basic_type(json_data):
            # Propably only a json value (aka json_data) was provided but not an attribute name (aka label_name)
            raise ValueError("Not a valid json: '{}'".format(json.dumps(json_data)))
        if self._is_basic_type(json_data):
            # we just have a simple str,int,float value that we turn into an node

            property_name = self._adjust_property_name(label_name_adjusted, label_name)
            node[property_name] = json_data
            node.__primarykey__ = property_name
        elif isinstance(json_data, list):

            if (
                self.config_bool_collection_anchor_only_when_len_min_2
                and len(json_data) == 1
            ):
                node = self._jsondict2subgraph(label_name, json_data[0], parent_node)
            else:

                if (
                    not self.config_list_skip_collection_hubs == "all"
                    and self._get_hub_node_label_name(node.__primarylabel__)
                    not in self.config_list_skip_collection_hubs
                ):
                    # we create a new Collection/Hub Node, with an id based on the list content hashed
                    node = self._create_collection_hub_node(
                        member_label_name=node.__primarylabel__,
                        hub_id=hashlib.md5(json.dumps(json_data).encode()).hexdigest(),
                    )
                else:
                    node = parent_node
                for index, list_item in enumerate(json_data):

                    list_item_node = self._jsondict2subgraph(
                        label_name, list_item, node
                    )

                    if list_item_node is not None:
                        if node is not None:
                            if list_item_node != node:
                                if (
                                    label_name
                                    in self.config_dict_json_attr_to_reltype_instead_of_label
                                ):
                                    override_val = {
                                        self.config_dict_json_attr_to_reltype_instead_of_label[
                                            label_name
                                        ]: label_name
                                    }
                                    node.override_reltype = override_val

                                self._create_relation(
                                    node, list_item_node, {"position": index},
                                )
                        # print("ADD AS LIST ITEM", list_item_node)
                        # self._add_node(list_item_node)

        elif isinstance(json_data, dict) and label_name is not None:
            attrs_to_child_nodes = []
            self._fold_json_attrs(label_name, json_data)
            for key, val in json_data.items():
                # val = self._fold_json_attrs(key, val)
                property_name = self._adjust_property_name(node.__primarylabel__, key)
                if self._is_basic_type(val):
                    node[property_name] = val
                    # Set default primary key if not allready occupied
                    if key in self.config_list_default_primarykeys and not hasattr(
                        node, "__primarykey__"
                    ):
                        node.__primarykey__ = property_name
                    # Set primary key as configured by caller
                    if (
                        node.__primarylabel__
                        in self.config_dict_primarykey_attr_by_label
                        and self.config_dict_primarykey_attr_by_label[
                            node.__primarylabel__
                        ]
                        == key
                    ):
                        node.__primarykey__ = property_name
                    # create extra node from property

                    if (
                        self.config_dict_property_to_extra_node is not None
                        and label_name_adjusted
                        in self.config_dict_property_to_extra_node
                        and key
                        in self.config_dict_property_to_extra_node[label_name_adjusted]
                    ):
                        attrs_to_child_nodes.append((key, val))
                elif isinstance(val, list) and key in self.config_dict_concat_list_attr:
                    node[property_name] = self.config_dict_concat_list_attr[key].join(
                        val
                    )
                else:
                    attrs_to_child_nodes.append((key, val))
            node = self._generate_id_attr(node, json_data)
            for attr_to_cn in attrs_to_child_nodes:
                child_node = self._jsondict2subgraph(attr_to_cn[0], attr_to_cn[1], node)

                if child_node is not None:
                    if child_node != node:
                        if (
                            label_name
                            in self.config_dict_json_attr_to_reltype_instead_of_label
                        ):
                            node.override_reltype = {
                                self.config_dict_json_attr_to_reltype_instead_of_label[
                                    label_name
                                ]: label_name
                            }

                        self._create_relation(node, child_node)
        elif isinstance(json_data, dict) and label_name is None:
            # we skip translating outermost layer into nodes and just transform the json content
            node = None
            for key, val in json_data.items():
                self._jsondict2subgraph(key, val, None)
        if callable(self.config_func_node_post_modifier):
            node = self.config_func_node_post_modifier(node)
        if node is not None:
            self._add_node(node)
        return node
