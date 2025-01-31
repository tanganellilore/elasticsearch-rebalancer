from collections import defaultdict
from fnmatch import fnmatch
import time
import json

import requests
import click
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

from humanize import naturalsize


class BalanceException(Exception):
    def __init__(self, message):
        # message = click.style(message, 'red')
        super(BalanceException, self).__init__(message)


def matches_attrs(attrs, match_attrs):
    for key, value in match_attrs.items():
        if attrs.get(key) != value:
            return False
    return True


def es_request(es_host, endpoint, method=requests.get, **kwargs):
    try:
        response = method(
        f'{es_host}/{endpoint}',
        verify=False,
        **kwargs,
        )
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        raise BalanceException(f'Failed to fetch data from ES: {e}')


def get_cluster_health(es_client):
    return es_client.cluster.health()


def get_cluster_settings(es_client):
    return  es_client.cluster.get_settings()

def get_nodes_attributes_map(es_client):
    nodes_attr = es_client.cat.nodeattrs(format='json')
    nodes_attr_map = {}

    for item in nodes_attr:
        attr = item['attr']
        value = item['value']
        node = item['node']

        if attr not in nodes_attr_map:
            nodes_attr_map[attr] = {value: [node]}
        else:
            if value not in nodes_attr_map[attr]:
                nodes_attr_map[attr][value] = [node]
            else:
                nodes_attr_map[attr][value].append(node)
    return nodes_attr_map


def check_cluster_health(es_client):
    health = get_cluster_health(es_client)

    if health['status'] != 'green':
        raise Exception('ES is not green!')

    relocating_shards = health['relocating_shards']
    if relocating_shards > 0:
        raise Exception(f'ES is already relocating {relocating_shards} shards!')


def wait_for_no_relocations(es_client, timeout, logger):

    while True:
        health = get_cluster_health(es_client)

        relocating_shards = health['relocating_shards']
        if not relocating_shards:
            break
        print_and_log(logger.info, f'Waiting {timeout}s for relocations to complete... {relocating_shards} shards remaining')
        time.sleep(timeout)


def execute_reroute_commands(es_client, commands):
    return es_client.cluster.reroute(commands=commands)


def get_transient_cluster_settings(es_client, paths):
    settings = get_cluster_settings(es_client)
    path_to_value = {}

    for path in paths:
        attrs = path.split('.')

        value = settings['transient']
        for attr in attrs:
            value = value.get(attr)
            if not value:
                path_to_value[path] = None
                break
        path_to_value[path] = value

    return path_to_value


def set_transient_cluster_settings(es_client, path_to_value):
    es_client.cluster.put_settings(transient=path_to_value)


def get_nodes(es_client, role="data", attrs=None):
    nodes = es_client.nodes.stats()['nodes']
    filtered_nodes = []

    recoveries = get_recovery(es_client)

    for node_id, node_data in nodes.items():
        if not matches_attrs(node_data.get('attributes'), attrs) or role not in node_data.get('roles', []):
            continue

        node_data['id'] = node_id
        node_data['recovery'] = []
        for recovery in recoveries:
            if recovery['source_node'] == node_data['name'] or recovery['target_node'] == node_data['name']:
                node_data['recovery'].append(recovery)

        node_data['weight'] = node_data.get('fs', {}).get('total', {}).get('total_in_bytes', 0) - node_data.get('fs', {}).get('total', {}).get('available_in_bytes', 0)

        node_data["total_shards"] = node_data.get("indices", {}).get("shard_stats", {}).get("total_count", 0)

        filtered_nodes.append(node_data)

    return filtered_nodes


def get_shard_size(shard):
    if shard['store']:
        return int(shard['store'])
    return 0


def format_shard_size(weight):
    return naturalsize(weight, binary=True)


def get_recovery(es_client):
    # _cat/recovery?v&active_only=true&h=index,shard,source_node,target_node,stage,bytes_percent,translog_ops_percent,time&s=source_node
    return es_client.cat.recovery(format='json',
                                  active_only=True,
                                  h='index,shard,source_node,target_node,stage,bytes_percent,translog_ops_percent,time',
                                  s='source_node')


def get_shards(
    es_client,
    attrs=None,
    index_name_filter=None,
    max_shard_size=None,
    get_shard_weight_function=get_shard_size,
):
    indices = es_client.indices.get_settings()

    filtered_index_names = []

    for index_name, index_data in indices.items():
        index_settings = index_data['settings']['index']
        index_attrs = (
            index_settings
            .get('routing', {})
            .get('allocation', {})
            .get('require', {})
        )
        if not matches_attrs(index_attrs, attrs):
            continue

        if index_name_filter and not fnmatch(index_name, index_name_filter):
            continue

        filtered_index_names.append(index_name)

    shards = es_client.cat.shards(format='json', bytes='b')

    filtered_shards = []

    for shard in shards:
        if (
            shard['state'] != 'STARTED'
            or shard['index'] not in filtered_index_names
            or (max_shard_size and get_shard_weight_function(shard) > max_shard_size)
        ):
            # logger.debug(f"skip shard: {shard['index']} {shard['shard']} {shard['node']} {shard['store']}")
            continue

        shard['id'] = f'{shard["index"]}-{shard["shard"]}'
        shard['weight'] = get_shard_weight_function(shard)

        filtered_shards.append(shard)
    # logger.debug(f"Tot shards: {len(shards)}")
    # logger.debug(f"Tot filtered shards: {len(filtered_shards)}")
    return filtered_shards


def combine_nodes_and_shards(nodes, shards):
    node_name_to_shards = defaultdict(list)
    index_to_node_names = defaultdict(list)
    shard_id_to_node_names = defaultdict(list)

    for shard in shards:
        node_name_to_shards[shard['node']].append(shard)
        index_to_node_names[shard['index']].append(shard['node'])
        shard_id_to_node_names[shard['id']].append(shard['node'])

    node_name_to_shards = {
        node_name: sorted(shards, key=lambda shard: shard['weight'])
        for node_name, shards in node_name_to_shards.items()
    }

    ordered_nodes = []

    # logger.debug(f"Total node shards: {total_shards}")
    for node in nodes:
        if node['name'] not in node_name_to_shards:
            node_name_to_shards[node['name']] = []

        # node['weight'] = sum(
        #     shard['weight'] for shard in node_name_to_shards[node['name']]
        # )

        ordered_nodes.append(node)

    ordered_nodes = sorted(ordered_nodes, key=lambda node: node['weight'])

    # min_weight = ordered_nodes[0]['weight']
    max_weight = ordered_nodes[-1]['weight']

    for node in ordered_nodes:
        node['weight_percentage'] = round((node['weight'] / max_weight) * 100, 2)

    return ordered_nodes, node_name_to_shards, index_to_node_names, shard_id_to_node_names


def parse_attr(attributes):
    attr_map = {}
    for a in attributes:
        try:
            key, value = a.split('=', 1)
        except ValueError:
            raise BalanceException('Invalid attr, specify as key=value!')
        attr_map[key] = value
    return attr_map


def extract_attrs(attrs, skip_attrs):
    extract_attr = {}
    if not attrs or not skip_attrs:
        return extract_attr

    for skip_attr in skip_attrs:
        if attrs.get(skip_attr):
            extract_attr[skip_attr] = attrs.get(skip_attr)
    return extract_attr


def find_node(nodes, node_name=None, skip_attr_map=None, max_recovery_per_node=None):
    if not isinstance(nodes, list):
        nodes = list(nodes)

    if not node_name:
        if not skip_attr_map:
            for node in nodes:
                if max_recovery_per_node and len(node.get('recovery', [])) >= max_recovery_per_node:
                    continue
                node['recovery'].append({'shard': 'new_shard_allocated'})
                return node
        else:
            for node in nodes:
                if max_recovery_per_node and len(node.get('recovery', [])) >= max_recovery_per_node:
                    continue
                if not matches_attrs(node.get('attributes'), skip_attr_map):
                    node['recovery'].append({'shard': 'new_shard_allocated'})
                    return node
            return None

    for node in nodes:
        if node['name'] == node_name:
            if max_recovery_per_node and len(node.get('recovery', [])) >= max_recovery_per_node:
                continue
            node['recovery'].append({'shard': 'new_shard_allocated'})
            return node

    return None


def check_skip_attr(curr_node, skip_attrs_list, skip_attr_map, map_id):
    same_attr = False
    for skip_attr in skip_attrs_list:
        curr_skip_attr = skip_attr_map.get(skip_attr)
        if curr_skip_attr and not same_attr:
            for node in curr_skip_attr.get((curr_node.get('attributes', {}).get(skip_attr, "None")), []):
                if node in map_id:
                    same_attr = True
    return same_attr


def attempt_to_find_swap(
    nodes, shards, used_shards, logger,
    max_node_name=None,
    min_node_name=None,
    format_shard_weight_function=format_shard_size,
    one_way=False,
    use_shard_id=False,
    skip_attrs_list=None,
    node_skip_attrs_map=None,
    max_recovery_per_node=None,
    min_diff=0,
):
    ordered_nodes, node_name_to_shards, index_to_node_names, shard_id_to_node_names = (
        combine_nodes_and_shards(nodes, shards)
    )

    max_node = find_node(reversed(ordered_nodes), node_name=max_node_name, max_recovery_per_node=max_recovery_per_node)
    if not max_node:
        print_and_log(logger.error, f"Not Found node: '{max_node_name}'. Skip this iteration")
        return None

    max_node_skip_attr_map = extract_attrs(max_node.get('attributes'), skip_attrs_list)
    min_node = find_node(ordered_nodes, node_name=min_node_name, skip_attr_map=max_node_skip_attr_map, max_recovery_per_node=max_recovery_per_node)
    if not min_node:
        print_and_log(logger.error, f"Not Found node: '{min_node_name}'. Skip this iteration")
        return None

    min_weight = min_node['weight']
    max_weight = max_node['weight']
    spread_used = round(max_weight - min_weight, 2)

    print_and_log(logger.info, f'> Weight used over {len(nodes)} nodes: \
min={format_shard_weight_function(min_weight)}, \
max={format_shard_weight_function(max_weight)}, \
spread={format_shard_weight_function(spread_used)}'
    )

    if min_diff:
        if spread_used < min_diff:
            print_and_log(logger.warning, f'Spread is less than min-diff: {format_shard_weight_function(spread_used)} < {format_shard_weight_function(min_diff)}')
            print_and_log(logger.info, 'Cluster is balanced...wait')
            return []


    max_node_shards = node_name_to_shards[max_node['name']]
    min_node_shards = node_name_to_shards[min_node['name']]

    for shard in reversed(max_node_shards):  # biggest to smallest shard
        if shard['id'] not in used_shards:

            # Find if the shard to be moved is in a node that has the same attributes to be skipped
            # e.g. if the replica shard is in a node that has the same rack as the node to be moved to, skip it
            if node_skip_attrs_map:
                if use_shard_id:
                    same_attr = check_skip_attr(min_node, skip_attrs_list, node_skip_attrs_map, shard_id_to_node_names[shard['id']])
                else:
                    same_attr = check_skip_attr(min_node, skip_attrs_list, node_skip_attrs_map, index_to_node_names[shard['index']])
            else:
                same_attr = False

            if (
                use_shard_id 
                and min_node['name'] not in shard_id_to_node_names[shard['id']]
                and not same_attr
            ):
                max_shard = shard
                break
            elif (
                not use_shard_id 
                and min_node['name'] not in index_to_node_names[shard['index']]
                and not same_attr
            ):
                max_shard = shard
                break
    else:
        raise BalanceException((
            'Could not find suitable large shard to move to '
            f'{max_node["name"]}!'
        ))

    for shard in min_node_shards:
        if shard['id'] not in used_shards:
            # Find if the shard to be moved is in a node that has the same attributes to be skipped
            # e.g. if the replica shard is in a node that has the same rack as the node to be moved to, skip it
            if node_skip_attrs_map:
                if use_shard_id:
                    same_attr = check_skip_attr(max_node, skip_attrs_list, node_skip_attrs_map, shard_id_to_node_names[shard['id']])
                else:
                    same_attr = check_skip_attr(max_node, skip_attrs_list, node_skip_attrs_map, index_to_node_names[shard['index']])
            else:
                same_attr = False

            if (
                use_shard_id 
                and max_node['name'] not in shard_id_to_node_names[shard['id']]
                and not same_attr
            ):
                min_shard = shard
                break
            elif (
                not use_shard_id 
                and max_node['name'] not in index_to_node_names[shard['index']]
                and not same_attr
            ):
                min_shard = shard
                break
    else:
        if not one_way:
            raise BalanceException((
                'Could not find suitable small shard to move to '
                f'{min_node["name"]}!'
            ))

    # Update shard + node info according to the reroutes
    used_shards.add(max_shard['id'])
    max_shard['node'] = min_node['name']
    min_node['weight'] += max_shard['weight']
    max_node['weight'] -= max_shard['weight']

    if not one_way:
        used_shards.add(min_shard['id'])
        min_shard['node'] = max_node['name']
        min_node['weight'] -= min_shard['weight']
        max_node['weight'] += min_shard['weight']

        if min_node['weight'] >= max_node['weight']:
            print_and_log(logger.warning, f' Min-node become biggerthan Maxnode with this reroute. We will skip it')
            return []

    if one_way:
        print_and_log(logger.info, f'> Recommended move for: {max_shard["id"]} ({format_shard_weight_function(max_shard["weight"])})')
    else:
        print_and_log(logger.info, f'> Recommended swap for: {max_shard["id"]} \
({format_shard_weight_function(max_shard["weight"])}) <> {min_shard["id"]} \
({format_shard_weight_function(min_shard["weight"])})')

    print_and_log(logger.info, f'  maxNode: {max_node["name"]} ({max_node["total_shards"]} shards) \
({format_shard_weight_function(max_weight)} -> {format_shard_weight_function(max_node["weight"])})')
    print_and_log(logger.info, f'  minNode: {min_node["name"]} ({min_node["total_shards"]} shards) \
({format_shard_weight_function(min_weight)} -> {format_shard_weight_function(min_node["weight"])})\n'
    )
    

    reroute_commands = [
        {
            'move': {
                'index': max_shard['index'],
                'shard': max_shard['shard'],
                'from_node': max_node['name'],
                'to_node': min_node['name'],
            },
        },
    ]

    if not one_way:
        reroute_commands.append({
            'move': {
                'index': min_shard['index'],
                'shard': min_shard['shard'],
                'from_node': min_node['name'],
                'to_node': max_node['name'],
            },
        })

    return reroute_commands  

def print_command(command, logger):
    args = command['move']
    print_and_log(logger.info, f'> Executing reroute of {args["index"]}-{args["shard"]} \
        from {args["from_node"]} -> {args["to_node"]}'
    )

def check_raise_health(es_client):
    # Check we're good to go
    try:
        check_cluster_health(es_client)
    except Exception as e:
        raise BalanceException(f'{e}')


def wait_cluster_health(es_client, timeout, logger):
    print_and_log(logger.info, "> Checking cluster health...")
    while True:
        try:
            check_cluster_health(es_client)
        except Exception as e:
            print_and_log(logger.warning, f"Cluster health check failed: {e}, Waiting for {timeout}s...")
            time.sleep(timeout)
        else:
            break

def execute_reroutes(es_client, commands, timeout, logger):
    try:
        print_and_log(logger.info, "> Executing reroute...")
        execute_reroute_commands(es_client, commands)
    except Exception as e:
        print_and_log(logger.info, f'Failed to execute reroute commands: {e}')
        return False
    # Parallel reroute worked - so just wait & return
    else:
        for command in commands:
            print_command(command, logger)
        print_and_log(logger.info, 'Waiting for relocations to complete...')
        wait_for_no_relocations(es_client, timeout, logger)
        return True


def print_node_shard_states(
    nodes,
    logger,
    format_shard_weight_function=format_shard_size,
):
    for node in nodes:
        print_and_log(logger, 
            f'\n> Node: {node["name"]},\
            \nshards: {node["total_shards"]},\
            \nweight: {format_shard_weight_function(node["weight"])} \
            ({node["weight_percentage"]})%'
        )

def print_and_log(logger, message):
    logger(message)

def sleep(seconds, logger):
    print_and_log(logger, f'Sleeping for {seconds} seconds...')
    time.sleep(seconds)
