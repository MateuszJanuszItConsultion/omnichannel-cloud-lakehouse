"""Generator entry point. v1 supports only --mode=bootstrap (day zero)."""

import argparse
from dataclasses import replace

from pyspark.sql import SparkSession

from omnichannel_cloud_lakehouse.generator.config import GeneratorConfig
from omnichannel_cloud_lakehouse.generator.pools import build_all_pools
from omnichannel_cloud_lakehouse.generator.population import build_population
from omnichannel_cloud_lakehouse.generator.truth_tables import (
    ensure_bootstrap_allowed,
    read_clock,
    write_bootstrap,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--mode", choices=["bootstrap"], required=True)
    parser.add_argument("--n-persons", type=int, default=None)
    args = parser.parse_args()

    cfg = GeneratorConfig()
    if args.n_persons is not None:
        # replace() re-runs __post_init__, so the override is validated too.
        cfg = replace(cfg, n_persons=args.n_persons)

    spark = SparkSession.builder.getOrCreate()

    # Guard before any expensive work.
    ensure_bootstrap_allowed(read_clock(spark, args.catalog), cfg.day_zero)

    pools = build_all_pools(spark, cfg.seed, cfg.pool_draws)
    population = build_population(spark, cfg, pools)
    write_bootstrap(spark, args.catalog, population, cfg.day_zero)

    print(f"bootstrap_complete catalog={args.catalog} day_zero={cfg.day_zero} n_persons_target={cfg.n_persons}")
