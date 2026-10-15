"""Generator entry point. v1 supports only --mode=bootstrap (day zero)."""

import argparse
from dataclasses import replace

from pyspark.sql import SparkSession

from omnichannel_cloud_lakehouse.generator import landing, render
from omnichannel_cloud_lakehouse.generator import truth_tables as truth
from omnichannel_cloud_lakehouse.generator.config import GeneratorConfig
from omnichannel_cloud_lakehouse.generator.pools import build_all_pools
from omnichannel_cloud_lakehouse.generator.population import build_population
from omnichannel_cloud_lakehouse.generator.source_records import build_source_records

CUSTOMER_CHANNELS = ("loyalty", "webshop")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--mode", choices=["bootstrap"], required=True)
    parser.add_argument("--n-persons", type=int, default=None)
    parser.add_argument("--git-commit", default="unknown")
    args = parser.parse_args()

    cfg = GeneratorConfig()
    if args.n_persons is not None:
        # replace() re-runs __post_init__, so the override is validated too.
        cfg = replace(cfg, n_persons=args.n_persons)

    spark = SparkSession.builder.getOrCreate()

    # Guard before any expensive work.
    truth.ensure_bootstrap_allowed(truth.read_clock(spark, args.catalog), cfg.day_zero)

    # 1. Population -> truth.person
    pools = build_all_pools(spark, cfg.seed, cfg.pool_draws)
    truth.write_person(args.catalog, build_population(spark, cfg, pools))

    # 2. Source records with channel noise. Reading the person table back
    #    (instead of reusing the DataFrame) avoids recomputing the population;
    #    on serverless there is no cache(), the Delta table is the materialization.
    population = spark.table(truth.table(args.catalog, "person"))
    noisy = render.apply_channel_noise(build_source_records(population, cfg), CUSTOMER_CHANNELS, cfg.seed)
    truth.write_source_records(args.catalog, render.truth_source_records(noisy))

    # 3. Channel files. Rendered from the noisy records written in step 2's plan;
    #    determinism guarantees the files match truth.source_record.
    loyalty_path = landing.write_loyalty_snapshot(render.render_loyalty_snapshot(noisy), args.catalog, cfg.day_zero)
    webshop_path = landing.write_webshop_customers(render.render_webshop_customers(noisy), args.catalog, cfg.day_zero)

    # 4. Clock last: commit marker.
    truth.write_clock(spark, args.catalog, cfg.day_zero)

    print(f"bootstrap_complete catalog={args.catalog} day_zero={cfg.day_zero}")
    print(f"loyalty_snapshot={loyalty_path}")
    print(f"webshop_customers={webshop_path}")
    print(f"git_commit={args.git_commit}")
