import time
from src.config import runtime
from src.db import raw, settings as dbs
from src.spark import pipeline as sp
from src.spark.spark_setup import session
from src.spark.stagelog import StageLog

db = dbs.load()
spark = session(**{
    "spark.ui.showConsoleProgress": "false",
    "spark.sql.shuffle.partitions": "1",
})
started = time.monotonic()
frame = raw.read(spark, db, [21, 22])
names = sp.ported(runtime.load().profile("forecast_balance").steps)
log = StageLog()
log.opening("raw ids 21,22 | job demo")
log.event("read", "read raw_transactions", rows=2)
cleaned = sp.run(frame, names, listener=log)
log.event("write", "upsert cleaned_transactions (skipped)", rows=2)
log.closing("2 rows cleaned")
print("TOTAL %.1fs" % (time.monotonic() - started))
