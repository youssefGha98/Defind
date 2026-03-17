META_KEY = "_meta.json"
JOBS_KEY = "_jobs.jsonl"
JOBS_SUMMARY_KEY = "_meta/jobs_summary.json"
JOBS_LOCK_KEY = "_jobs.lock.json"
META_VERSION = 1
RUNNING_STATUSES = frozenset({"running"})
TERMINAL_STATUSES = frozenset({"completed", "failed", "stopped"})
IMMUTABLE_META_FIELDS = frozenset({"protocol", "contract", "start_block"})
UNSUPPORTED_LOCK_MARKERS = (
    "requires boto3",
    "botocore client support",
    "unsupported",
    "if-none-match",
    "if-match",
    "conditional",
    "precondition",
    "not implemented",
    "501",
)
