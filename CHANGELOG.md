# 1.0

+ Add `--use-shard-id` to use shard id for algorithm rebalance instead index name
+ Add `--skip-attr` used to exclude migration between two nodes that have same attributes
+ Add `--max-shard-size` to skip shards larger than a certain size
+ Add `--node-role` to filter nodes by their role
+ add `--max-recovery-per-node` to avoid moving shards to a node that is envolved in recovery
+ Enhanced the skip-attr check, to avoid swap of a shard that can have a replica in a node that have same attribute (used for some costrain). 
  

# 0.6

+ Add CLI option to temporarily override disk watermarks
+ Print out any unexpected error from ES in reroute operation
+ Drop support for python <= 3.5

# 0.5

+ Only check user provided attributes

# v0.4

+ Don't attempt to re-use shards already moved
+ Add `--one-way` to help where shard count is imbalanced (perhaps for a subset of indices)
+ Improve printing of node/shard state
+ Stop optimising shards when there are no longer improvements

# v0.3

+ Add `--index-name` wildcard filter

# v0.2

+ Make it possible to provide multiple `--min-node` and `--max-node`

# v0.1

+ First pass at ES rebalancing script
