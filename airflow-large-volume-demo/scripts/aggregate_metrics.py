import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple


@dataclass(frozen=True)
class Config:
    raw_dir: str
    output_path: str
    shards: int


def _date_key_from_iso(iso_dt: str) -> str:
    # ISO format: YYYY-MM-DDTHH:MM:SS...
    # Queremos a parte da data para agregar por dia
    return iso_dt.split("T", 1)[0]


def aggregate_from_shards(cfg: Config) -> None:
    raw_dir = Path(cfg.raw_dir)
    out_path = Path(cfg.output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # metrics[day] = (count, sum_amount)
    count_by_day: Dict[str, int] = defaultdict(int)
    sum_by_day: Dict[str, float] = defaultdict(float)

    for shard_idx in range(cfg.shards):
        shard_path = raw_dir / f"transactions_shard_{shard_idx:03d}.csv"
        if not shard_path.exists():
            # Caso rodem com menos shards do que o DAG espera
            continue

        with shard_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                event_time = row["event_time"]
                day = _date_key_from_iso(event_time)
                amount = float(row["amount"])

                count_by_day[day] += 1
                sum_by_day[day] += amount

    # Salva dataset pequeno para BI: metrics diarias
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["day", "event_count", "sum_amount", "avg_amount"])

        for day in sorted(count_by_day.keys()):
            cnt = count_by_day[day]
            total = sum_by_day[day]
            avg = total / cnt if cnt else 0.0
            writer.writerow([day, cnt, round(total, 2), round(avg, 4)])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--shards", type=int, default=10)
    args = parser.parse_args()

    cfg = Config(
        raw_dir=args.raw_dir,
        output_path=args.output_path,
        shards=args.shards,
    )
    aggregate_from_shards(cfg)
    print(f"OK: agregadas métricas em {cfg.output_path}")


if __name__ == "__main__":
    main()

