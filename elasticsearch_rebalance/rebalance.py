from collections import deque
import json
import logging
import warnings

import click
from elasticsearch import Elasticsearch

from . import utils


# create logger with loglevel
logger = logging.getLogger("elasticsearch_rebalance")
logger.setLevel(logging.INFO)

# print log to console
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

@click.command()
@click.argument('es_host')
# add auth params
@click.option('--es-user', envvar='ES_USER', default=None, help='Elasticsearch user.')
@click.option('--es-password', envvar='ES_PASSWORD', default=None, help='Elasticsearch password.')

@click.option('--iterations', default=1, type=int, help='Number of iterations (swaps) to execute.')
@click.option('--attr', multiple=True, help=(
        'Rebalance only on node with attributes specified here. '
        'Attributes is accepted in format key=value.'
    ))
@click.option('--commit', default=False, is_flag=True, help='Execute the shard reroutes (default print only).')
@click.option('--print-state', default=False, is_flag=True, help='Print the current nodes & weights and exit.')
@click.option('--index-name', default=None, help='Filter the indices for swaps by name, supports wildcards.')
@click.option('--max-node', default=None, multiple=True, help='Force the max node to consider for shard swaps.')
@click.option('--min-node', default=None, multiple=True, help='Force the min node to consider for shard swaps.')
@click.option('--one-way', default=False, is_flag=True, help=(
        'Disables shard swaps and simply moves max -> min. '
        'Note after ES rebalancing is restored ES will attempt '
        "to rebalance itself according to it's own heuristics."
    ))
@click.option('--override-watermarks', default=False, is_flag=True, help=(
        'Temporarily override the Elasticsearch low & high disk '
        'watermark settings. Makes it possible to parallel swap '
        'shards even when the most full nodes are on the limit.'
    ))
@click.option('--use-shard-id', default=False, is_flag=True, help=(
        'If passed, we use an internal unique shard_id instead '
        'index name for shard algoritms. Without this params if index '
        'of shard is in the max and min node, shard will be skipped. To be used '
        'when indices have shards and you want to rebalance shards between nodes.'
    ))
@click.option('--skip-attr', multiple=True, help=(
        'If specified we avoid rebalance beetween node that have same '
        'attributes specified here. Attributes are in string format and separated by space. '
        'Example: --skip-attr rack_id key_1 key_2'
    ))
@click.option('--max-shard-size', default=None, type=int,
    help='Max shard size in bytes. If a shard is larger than this, it will be skipped.')
@click.option('--node-role', default='data', help=(
        'Filter the nodes for swaps by role. Typically this are the roles '
        'defined in the elasticsearch.yml file. Generally you can use this '
        '"hot" or "warm" or "cold" to filter nodes by their role.'
        'Default is "data", which means all data are considered for rebalance.'
    ))
@click.option('--max-recovery-per-node', default=None, type=int, help=(
    'Max number of concurrent recoveries per node. '
    'If a node has more recoveries, it will be skipped.'
    ))
@click.option('--infinite-loop', default=False, is_flag=True, help='Run the rebalance in infinite loop.')
@click.option('--min-diff', default=0, type=int, help='Min difference in bytes between nodes to consider rebalance.')
@click.option('--disable-rebalance', default=False, is_flag=True, help=(
        'Set cluster.routing.rebalance.enable to none before rebalance and restore after. '
        'Generally should be passed and will be used only if --commit is passed.'
    ))
@click.option('--timeout', default=60, type=int, help='Timeout in seconds for Elasticsearch requests and cluster health check.')


def rebalance_elasticsearch(
        es_host,
        es_user=None,
        es_password=None,
        iterations=1,
        used_shards=None,
        attr=None,
        commit=False,
        print_state=False,
        index_name=None,
        max_node=None,
        min_node=None,
        one_way=False,
        override_watermarks=None,
        use_shard_id=False,
        skip_attr=None,
        max_shard_size=None,
        node_role="data",
        max_recovery_per_node=None,
        infinite_loop=False,
        min_diff=0,
        disable_rebalance=False,
        timeout=60,
):
    if es_user and es_password:
        es_client = Elasticsearch(
            es_host, basic_auth=(es_user, es_password), verify_certs=False, ssl_show_warn=False,
            request_timeout=timeout)
    else:
        es_client = Elasticsearch(es_host, verify_certs=False, ssl_show_warn=False, request_timeout=timeout)


    # Parse out any attrs
    attrs = {}
    if attr:
        attrs = utils.parse_attr(attr)

    # Parse out any skip_attrs
    skip_attrs = []
    if skip_attr:
        skip_attrs = skip_attr

    # Turn min/max node lists into deque instances
    if min_node:
        min_node = deque(min_node)
    if max_node:
        max_node = deque(max_node)

    utils.print_and_log(logger.info, '# Elasticsearch Rebalancer')
    utils.print_and_log(logger.info, f'> User: {es_user}')
    utils.print_and_log(logger.info, f'> Target: {click.style(es_client, bold=True)}')

    if commit:
        if print_state:
            raise click.ClickException('Cannot have --commit and --print-state!')

        # Check we have a healthy cluster
        utils.wait_cluster_health(es_client, timeout, logger)

        settings_to_set = {}
        if disable_rebalance:
            utils.print_and_log(logger.info, 'Disabling cluster rebalance...')
            settings_to_set = {'cluster.routing.rebalance.enable': 'none'}

        if override_watermarks:
            utils.print_and_log(logger.info, f'Overriding disk watermarks to: {override_watermarks}')
            settings_to_set.update({
                'cluster.routing.allocation.disk.watermark.low': override_watermarks,
                'cluster.routing.allocation.disk.watermark.high': override_watermarks,
            })

        # Save the old value to restore later
        previous_settings = utils.get_transient_cluster_settings(es_client, settings_to_set.keys())

        if disable_rebalance or override_watermarks:
            utils.set_transient_cluster_settings(es_client, settings_to_set)

    try:
        utils.print_and_log(logger.debug, 'Loading nodes...')
        nodes = utils.get_nodes(es_client, role=node_role, attrs=attrs)
        if not nodes:
            utils.print_and_log(logger.error, 'No nodes found! Exit')
            exit(1)

        utils.print_and_log(logger.info, f'> Founded {len(nodes)} nodes')

        utils.print_and_log(logger.debug, 'Loading shards...')
        shards = utils.get_shards(
            es_client,
            attrs=attrs,
            index_name_filter=index_name,
            max_shard_size=max_shard_size
        )
        if not shards:
            utils.print_and_log(logger.error, 'No shards found! Exit')
            exit(1)

        utils.print_and_log(logger.info, f'> Founded {len(shards)} shards')

        if print_state:
            utils.print_and_log(logger.info, 'Nodes ordered by weight:')
            utils.print_node_shard_states(nodes, logger.info)
            return

        utils.print_and_log(logger.debug, 'Investigating rebalance options...')

        if skip_attrs:
            node_skip_attrs_map = utils.get_nodes_attributes_map(es_client)
        else:
            node_skip_attrs_map = None

        all_reroute_commands = []
        if used_shards is None:
            used_shards = set()

        for i in range(iterations):
            utils.print_and_log(logger.info, f'> Iteration {i}')
            reroute_commands = utils.attempt_to_find_swap(
                nodes, shards, used_shards, logger,
                max_node_name=max_node[0] if max_node else None,
                min_node_name=min_node[0] if min_node else None,
                one_way=one_way,
                use_shard_id=use_shard_id,
                skip_attrs_list=skip_attrs,
                node_skip_attrs_map=node_skip_attrs_map,
                max_recovery_per_node=max_recovery_per_node,
                min_diff=min_diff,
            )

            if reroute_commands is not None:
                all_reroute_commands.extend(reroute_commands)
            else:
                break

            if min_node:
                min_node.rotate()
            if max_node:
                max_node.rotate()

        if commit:
            reroute_result = utils.execute_reroutes(es_client, all_reroute_commands, timeout, logger)
            if not reroute_result:
                raise utils.BalanceException('Error during reroute')
            else:
                utils.print_and_log(logger.info, '# Reroute Performed')

            if infinite_loop:
                utils.print_and_log(logger.info, f'# Infinite loop enabled. Sleeping for {timeout} seconds before next iteration...')
                utils.sleep(timeout, logger)
                rebalance_elasticsearch(
                    es_client,
                    es_user=es_user,
                    es_pwd=es_password,
                    iterations=iterations,
                    used_shards=used_shards,
                    attr=attr,
                    commit=commit,
                    print_state=print_state,
                    index_name=index_name,
                    max_node=max_node,
                    min_node=min_node,
                    one_way=one_way,
                    override_watermarks=override_watermarks,
                    use_shard_id=use_shard_id,
                    skip_attr=skip_attr,
                    max_shard_size=max_shard_size,
                    node_role=node_role,
                    max_recovery_per_node=max_recovery_per_node,
                    infinite_loop=infinite_loop,
                    min_diff=min_diff,
                    disable_rebalance=disable_rebalance,
                    timeout=timeout
                )
            else:
                utils.print_and_log(logger.info, '# Infinite loop disabled. Exiting...')
        else:
            utils.print_and_log(logger.info, '> No Command will be executed. Below the POST to be executed for reroute:')
            if not all_reroute_commands or len(all_reroute_commands) == 0:
                utils.print_and_log(logger.info, 'No reroute commands to execute.')
            else:
                utils.print_and_log(logger.info, '> Command:  \nPOST /_cluster/reroute \n{ \n"commands": \n' + json.dumps(all_reroute_commands)+'\n}')

    except Exception as e:
        utils.print_and_log(logger.error, f"Error on rebalance: {e}")
        exit(1)

    # Always restore the previous rebalance setting
    finally:
        if commit:
            if disable_rebalance or override_watermarks:
                utils.print_and_log(logger.info, f'Restoring previous settings ({previous_settings})...')
                utils.set_transient_cluster_settings(es_client, previous_settings)

    if commit:
        utils.print_and_log(logger.info, f'# Ended rebalanced. Executed {len(all_reroute_commands)} reroutes in last run!')
    else:
        utils.print_and_log(logger.info, f'# Ended rebalanced. Calculated {len(all_reroute_commands)} reroutes!')


if __name__ == '__main__':
    rebalance_elasticsearch(auto_envvar_prefix='ES_REBALANCE')
