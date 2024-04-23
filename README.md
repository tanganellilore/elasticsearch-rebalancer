# Elasticsearch Rebalancer

A script that attempts to re-balance Elasticsearch shards by size without changing the existing balancing.

By default ES balances shards over nodes by considering:

+ The number of shards / node
+ The number of shards / index / node

Which is great if every shard is the same size, but in reality this is not the case. Without considering the size of shards (except for watermarks) it's possible to end up with some nodes almost at watermark alongside others that are almost empty. 
This diagram highlights the problem `elasticsearch-rebalancer` attempts to solve:

![](es-rebalancer.png)


## How does it work?

The script is based around the idea of "swaps" - pairs of shards to relocate between the two nodes. Each iteration the script identifies the most-full and least-full nodes, searching through their largest/smallest shards to find a suitable swap. Ideally the `largest shard on the most-full node` and the `smallest shard on the least-full node` swap.

To maintain existing ES balances, shards are only considered if the node to move to does not have any other shard from the same index. This means the shards per node and shards per index per node remain the same, so ES shouldn't do any additional rebalancing.

## Usage

```
Usage: es-rebalance [OPTIONS] ES_HOST

Options:
  --iterations INTEGER  Number of iterations (swaps) to execute.
  --attr TEXT           Rebalance only on node with attributes specified here. 
                        Attributes is accepted in format key=value.
  --commit              Execute the shard reroutes (default print only).
  --print-state         Print the current nodes & weights and exit.
  --index-name TEXT     Filter the indices for swaps by name, supports
                        wildcards.
  --max-node TEXT       Force the max node to consider for shard swaps.
  --min-node TEXT       Force the min node to consider for shard swaps.
  --one-way             Disables shard swaps and simply moves max -> min. Note
                        after ES rebalancing is restored ES will attempt to
                        rebalance itself according to it's own heuristics.
  --use-shard-id        If passed, we use the shard_id created in runtime instead
                        index name for shard algoritms. Without this params if index
                        of shard is in the max and min node, shard will be skipped.
  --skip-attr            If specified we avoid rebalance beetween node that have same 
                        attributes specified here. Attributes are in string format.
  --help                Show this message and exit.
```

