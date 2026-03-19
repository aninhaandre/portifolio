import argparse
import csv
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


@dataclass(frozen=True)
class Config:
    out_dir: str
    shards: int
    rows_per_shard: int
    seed: int
    start_date: str  # YYYY-MM-DD
    days: int


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def generate_shard(
    *,
    out_dir: Path,
    shard_idx: int,
    cfg: Config,
) -> Path:
    """
    Gera um CSV com volume grande de transacoes sintéticas.
    Retorna o caminho do arquivo gerado.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Reprodutibilidade por shard: seed global + shard index
    rng = random.Random(cfg.seed + shard_idx)

    shard_path = out_dir / f"transactions_shard_{shard_idx:03d}.csv"
    start_dt = _parse_date(cfg.start_date)

    user_id_max = 200_000  # mantem cardinalidade razoavel para agregacoes diarias
    event_types = ("purchase", "refund", "subscription")

    with shard_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Schema
        writer.writerow(["event_time", "user_id", "event_type", "amount"])

        # Data range: cfg.days dias
        for i in range(cfg.rows_per_shard):
            day_offset = rng.randrange(cfg.days)
            # time dentro do dia (segundos)
            seconds_in_day = 24 * 60 * 60
            seconds_offset = rng.randrange(seconds_in_day)
            event_dt = start_dt + timedelta(days=day_offset, seconds=seconds_offset)

            user_id = rng.randrange(1, user_id_max + 1)
            event_type = rng.choice(event_types)

            # quantidade/amount sintética com distribuição simples
            base = rng.random() * 200.0
            if event_type == "refund":
                amount = -base
            else:
                amount = base

            # amount com 2 casas decimais
            amount = round(amount, 2)

            writer.writerow([event_dt.isoformat(), user_id, event_type, amount])

    return shard_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--shards", type=int, default=10)
    parser.add_argument("--rows-per-shard", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()

    cfg = Config(
        out_dir=args.out_dir,
        shards=args.shards,
        rows_per_shard=args.rows_per_shard,
        seed=args.seed,
        start_date=args.start_date,
        days=args.days,
    )

    out_dir = Path(cfg.out_dir)
    for shard_idx in range(cfg.shards):
        generate_shard(out_dir=out_dir, shard_idx=shard_idx, cfg=cfg)

    print(f"OK: gerados {cfg.shards} shards em {out_dir}")


if __name__ == "__main__":
    main()

