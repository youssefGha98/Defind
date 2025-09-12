from __future__ import annotations

import os, json
from typing import List, Tuple, Optional

def merge_intervals(intervals: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
    if not intervals: return []
    ivs = sorted(intervals); out: List[List[int]] = [[ivs[0][0], ivs[0][1]]]
    for s,e in ivs[1:]:
        ms,me = out[-1]
        if s <= me+1: out[-1][1] = max(me,e)
        else: out.append([s,e])
    return [(s,e) for s,e in out]

def subtract_iv(iv: Tuple[int,int], covered: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
    s,e = iv
    if s>e: return []
    if not covered: return [iv]
    res: List[Tuple[int,int]] = []; cur = s
    for cs,ce in covered:
        if ce<cur: continue
        if cs>e: break
        if cs>cur: res.append((cur, min(e, cs-1)))
        cur = max(cur, ce+1)
        if cur>e: break
    if cur<=e: res.append((cur,e))
    return res

def load_done_coverage(manifests_dir: str, exclude_basename: Optional[str]) -> List[Tuple[int,int]]:
    ivs: List[Tuple[int,int]] = []
    if not os.path.isdir(manifests_dir): return []
    for name in os.listdir(manifests_dir):
        if not name.endswith(".jsonl"): continue
        if exclude_basename and name == exclude_basename: continue
        path = os.path.join(manifests_dir, name)
        try:
            with open(path, "r") as f:
                for line in f:
                    if not line.strip(): continue
                    rec = json.loads(line)
                    if rec.get("status") == "done":
                        ivs.append((int(rec["from_block"]), int(rec["to_block"])))
        except Exception:
            continue
    return merge_intervals(ivs)
