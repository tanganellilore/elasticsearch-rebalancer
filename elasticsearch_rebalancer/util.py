from collections import defaultdict
from fnmatch import fnmatch
from time import sleep

import requests
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

from humanize import naturalsize


def matches_attrs(attrs, match_attrs):
    for key, value in match_attrs.items():
        if attrs.get(key) != value:
            return False
    return True


def es_request(es_host, endpoint, method=requests.get, **kwargs):
    response = method(
        f'{es_host}/{endpoint}',
        verify=False,
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def get_cluster_health(es_host):
    return es_request(es_host, '_cluster/health')


def get_cluster_settings(es_host):
    return es_request(es_host, '_cluster/settings')

def get_nodes_attributes_map(es_host):
    nodes_attr = es_request(es_host, '_cat/nodeattrs?v=true&format=json')
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


def check_cluster_health(es_host):
    health = get_cluster_health(es_host)

    if health['status'] != 'green':
        raise Exception('ES is not green!')

    relocating_shards = health['relocating_shards']
    if relocating_shards > 0:
        raise Exception(f'ES is already relocating {relocating_shards} shards!')


def wait_for_no_relocations(es_host):
    while True:
        health = get_cluster_health(es_host)

        relocating_shards = health['relocating_shards']
        if not relocating_shards:
            break

        sleep(10)


def execute_reroute_commands(es_host, commands):
    es_request(es_host, '_cluster/reroute', method=requests.post, json={
        'commands': commands,
    })


def get_transient_cluster_settings(es_host, paths):
    settings = get_cluster_settings(es_host)
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


def set_transient_cluster_settings(es_host, path_to_value):
    es_request(es_host, '_cluster/settings', method=requests.put, json={
        'transient': path_to_value,
    })


def get_nodes(es_host, role="data", attrs=None):
    nodes = es_request(es_host, '_nodes/stats/fs')['nodes']
    filtered_nodes = []

    recoveries = get_recovery(es_host)

    for node_id, node_data in nodes.items():
        if not matches_attrs(node_data.get('attributes'), attrs) or role not in node_data.get('roles', []):
            continue

        node_data['id'] = node_id
        node_data['recovery'] = []
        for recovery in recoveries:
            if recovery['source_node'] == node_data['name'] or recovery['target_node'] == node_data['name']:
                node_data['recovery'].append(recovery)

        filtered_nodes.append(node_data)

    return filtered_nodes


def get_shard_size(shard):
    if shard['store']:
        return int(shard['store'])
    return 0


def format_shard_size(weight):
    return naturalsize(weight, binary=True)
 
def get_recovery(es_host):
    # _cat/recovery?v&active_only=true&h=index,shard,source_node,target_node,stage,bytes_percent,translog_ops_percent,time&s=source_node
    return es_request(es_host, '_cat/recovery', params={
        'active_only': 'true',
        'h': 'index,shard,source_node,target_node,stage,bytes_percent,translog_ops_percent,time',
        's': 'source_node',
        'format': 'json',
    })


def get_shards(
    es_host,
    attrs=None,
    index_name_filter=None,
    max_shard_size=None,
    get_shard_weight_function=get_shard_size,
):
    indices = es_request(es_host, '_settings')

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

    shards = es_request(
        es_host, '_cat/shards',
        params={
            'format': 'json',
            'bytes': 'b',
        },
    )

    filtered_shards = []

    for shard in shards:
        if (
            shard['state'] != 'STARTED'
            or shard['index'] not in filtered_index_names
            or (max_shard_size and get_shard_weight_function(shard) > max_shard_size)
        ):
            continue

        shard['id'] = f'{shard["index"]}-{shard["shard"]}'
        shard['weight'] = get_shard_weight_function(shard)

        filtered_shards.append(shard)
    print("shards", shards)
    print("filtered_shards", len(filtered_shards))
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
    total_shards = sum(len(shards) for shards in node_name_to_shards.values())
    print("total_shards", total_shards)
    for node in nodes:
        if node['name'] not in node_name_to_shards:
            continue

        node['weight'] = sum(
            shard['weight'] for shard in node_name_to_shards[node['name']]
        )

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
