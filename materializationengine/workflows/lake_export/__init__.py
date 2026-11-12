"""The parallel Delta Lake export: a table is read as id ranges at once, each range is
committed to each lake in one transaction, and lakes are optimized a few partitions per
task. Design and measurements: DELTA_LAKE_PARALLEL_EXPORT.md; plan: DELTA_LAKE_EXPORT_PLAN.md.

- sizing: how many ranges a table is split into, how big a stream's buffer is, and how a
  lake's optimize is divided into tasks -- all from the table's size, so a small table
  keeps one stream and a large one spreads over many.
- ranges: the SELECT for one id range, and streaming a range into staging tables inside
  each lake through the export's _flush_buffer.
- publish: a staged range's files added to its lake in one commit, tagged with the job
  and the range so a restarted job can tell what is already in.
- optimize: z-order (or compact) of a group of partitions in its own commit, deleting the
  files it replaced.
- storage: idempotent, retried, parallel deletes under a lake.
"""
