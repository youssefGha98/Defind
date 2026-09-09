# Ownership Batch

Batch tool to index only ownership and admin-style events for a list of EVM
contracts.

The tool lives outside the main `defind` CLI on purpose so we can iterate
without polluting the primary operator workflow.

## What It Indexes

By default, the tool keeps only standard governance and control events:

- `OwnershipTransferred`
- `OwnershipTransferStarted`
- `AdminChanged`
- `RoleAdminChanged`
- `RoleGranted`
- `RoleRevoked`
- `DefaultAdminTransferScheduled`
- `DefaultAdminTransferCanceled`
- `DefaultAdminDelayChangeScheduled`
- `DefaultAdminDelayChangeCanceled`

If a contract ABI does not expose any of these events, it is skipped and logged
in the batch report.

## Input CSV

Required columns:

- `address`
- `protocol_slug`
- `contract_slug`
- `start_block`

Optional columns:

- `end_block`

Example:

```csv
address,protocol_slug,contract_slug,start_block,end_block
0x820FB8127a689327C863de8433278d6181123982,arrakis,meta_vault_factory,20540819,latest
0x31CcDb5bd6322483bebD0787e1DABd1Bf1f14946,gamma,hyper_registry,13659998,latest
```

## Run

```bash
cd /home/gharbi/Defind
.venv/bin/python ownership_batch/run.py \
  --csv-path ownership_batch/contracts.csv \
  --rpc-url 'https://eth-mainnet.g.alchemy.com/v2/...' \
  --storage local
```

Outputs are written by default under:

```text
ownership_batch/data/<protocol_slug>/<contract_slug>
```

The batch report is written to:

```text
ownership_batch/data/_meta/ownership_batch_report.json
```

## Notes

- ABI fetch currently uses the existing Etherscan integration from the repo.
- `ETHERSCAN_API_KEY` can be provided in `.env`.
- S3 configuration reuses the same `.env` variables as the main CLI.
