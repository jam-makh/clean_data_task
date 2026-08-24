import time
from src.config import runtime
from src.config.policy import load as load_policy
from src.db import raw, settings as dbs
from src.spark import pipeline as sp
from src.spark.spark_setup import session

db = dbs.load()
spark = session(**{
    "spark.ui.showConsoleProgress": "false",
    "spark.sql.shuffle.partitions": "1",
})
names = sp.ported(runtime.load().profile("forecast_balance").steps)
policy = load_policy()

for attempt in (1, 2, 3):
    started = time.monotonic()
    frame = raw.read(spark, db, [21, 22])
    cleaned = sp.run(frame, names, policy=policy)
    rows = cleaned.count()
    print(f"RESULT pass {attempt}: {rows} rows in {time.monotonic()-started:.1f}s")
