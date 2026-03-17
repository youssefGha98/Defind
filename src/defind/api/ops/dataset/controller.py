from __future__ import annotations

import time

from fastapi import HTTPException

from defind.api.ops.dataset.models import (
    DatasetCoverageGapReadModel,
    DatasetCoverageReadModel,
    DatasetCreatePostModel,
    DatasetReadModel,
    DatasetUpdatePatchModel,
)
from defind.api.ops.dataset.repository import DatasetRepository
from defind.api.ops.dataset.utils import build_registry_from_inputs, dataset_from_route, normalize_dataset_patch
from defind.api.ops.shared.models import DatasetRef
from defind.api.ops.shared.utils import is_hex_address, to_iso_z
from defind.dataset_state import META_KEY, build_dataset_meta
from defind.orchestration.validator import validate_coverage


class DatasetController:
    def __init__(self, *, repository: DatasetRepository) -> None:
        self._repository = repository

    async def list_datasets(
        self,
        *,
        protocol_slug: str | None = None,
        contract_slug: str | None = None,
    ) -> list[DatasetReadModel]:
        return [
            DatasetReadModel.model_validate(row)
            for row in self._repository.list_cached_dataset_rows(
                protocol_slug=protocol_slug,
                contract_slug=contract_slug,
            )
        ]

    async def create_dataset(self, payload: DatasetCreatePostModel) -> DatasetReadModel:
        protocol = payload.protocol.strip()
        contract = payload.contract.strip()
        address = payload.contract_address.strip()
        if not protocol or not contract:
            raise HTTPException(status_code=400, detail="protocol and contract are required")
        if not is_hex_address(address):
            raise HTTPException(status_code=400, detail="invalid contract address")

        dataset = DatasetRef(protocol=protocol, contract=contract)
        storage, _ = self._repository.build_dataset_storage(dataset)
        if storage.exists(META_KEY):
            raise HTTPException(status_code=409, detail="dataset already exists")

        try:
            registry, selected_names = build_registry_from_inputs(
                abi_path=payload.abi_path,
                abi_json=payload.abi_json,
                registry_json=payload.registry_json,
                event_names=payload.event_names,
            )
            meta = build_dataset_meta(
                protocol=dataset.protocol,
                contract=dataset.contract,
                contract_address=address,
                chain_id=payload.chain_id,
                start_block=payload.start_block,
                chunk_size=payload.chunk_size,
                step=payload.step,
                storage=payload.storage,
                rpc_url=payload.rpc_url,
                event_names=selected_names,
                registry=registry,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            created, _ = self._repository.create_dataset(dataset, meta)
        except FileExistsError as exc:
            raise HTTPException(status_code=409, detail="dataset already exists") from exc

        return DatasetReadModel.model_validate(
            self._repository.refresh_cached_dataset_row(dataset, meta=created)
        )

    async def get_dataset(self, protocol: str, contract: str) -> DatasetReadModel:
        dataset = dataset_from_route(protocol, contract)
        row = self._repository.get_cached_dataset_row(dataset)
        if row is None:
            row = self._repository.refresh_cached_dataset_row(dataset)
        return DatasetReadModel.model_validate(row)

    async def patch_dataset(
        self,
        protocol: str,
        contract: str,
        payload: DatasetUpdatePatchModel,
    ) -> DatasetReadModel:
        dataset, _, _ = self._repository.get_context(protocol, contract)
        patch_payload = {field: getattr(payload, field) for field in payload.model_fields_set}
        normalized_payload = normalize_dataset_patch(patch_payload)
        updated = self._repository.update_meta(dataset, normalized_payload)
        row = self._repository.refresh_cached_dataset_row(dataset, meta=updated)
        return DatasetReadModel.model_validate(row)

    async def get_coverage(self, protocol: str, contract: str) -> DatasetCoverageReadModel:
        dataset, meta, storage = self._repository.get_context(protocol, contract)
        event_names = meta.get("event_names")
        if not isinstance(event_names, list) or not event_names:
            raise HTTPException(status_code=400, detail="dataset meta is missing event_names")
        start_block = int(meta.get("start_block") or 0)
        row = self._repository.build_dataset_row(dataset, meta)
        chain_head = int(row.get("chain_head") or 0)
        if chain_head < start_block:
            return DatasetCoverageReadModel(complete=False, gaps=[], invalid_chunks={})

        report = validate_coverage(
            storage=storage,
            event_names=[str(item) for item in event_names],
            start_block=start_block,
            end_block=chain_head,
        )
        detected_at = to_iso_z(int(time.time()))
        gaps = [
            DatasetCoverageGapReadModel(
                range_start=gap_start,
                range_end=gap_end,
                missing_blocks=max(0, gap_end - gap_start + 1),
                detected_at=detected_at,
            )
            for gap_start, gap_end in report.missing_in_range
        ]
        return DatasetCoverageReadModel(
            complete=report.is_valid and len(gaps) == 0,
            gaps=gaps,
            invalid_chunks=report.invalid_chunks_by_event,
        )
