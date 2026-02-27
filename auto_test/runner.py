"""ACME Inc parallel text-flow simulator.

Runs N conversations against Core's HTTP endpoint:
  POST /test/text_turn

Architecture boundary:
- This module is tenant-specific (ACME Inc) and therefore lives under
  `toolkits/acme_inc/auto_test/`.
- It may *use* OpenAI via `utils.openai_simulation`, but it must not implement
  OpenAI client logic itself.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# When executing this file directly (python toolkits/.../runner.py), Python sets
# sys.path[0] to the script directory, which can break absolute imports like
# `from toolkits...`. Ensure repo root is in sys.path.
# runner.py is at: <repo>/toolkits/acme_inc/auto_test/runner.py
# parents[3] => <repo>
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _load_env_file(path: str) -> None:
    """Load KEY=VALUE env file into os.environ (setdefault semantics)."""

    try:
        from quake_slm_outbound.core.env_profile import load_env_file

        load_env_file(path)
        return
    except Exception:
        pass

    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception:
        return

    for raw in lines:
        line = (raw or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k:
            os.environ.setdefault(k, v)


def _resolve_env_file(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        return explicit
    if os.path.exists("mock.env.local"):
        return "mock.env.local"
    return None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _parse_json_obj(s: Optional[str]) -> Dict[str, Any]:
    if not s:
        return {}
    try:
        obj = json.loads(s)
    except Exception as e:
        raise SystemExit(f"Invalid JSON: {e}")
    if not isinstance(obj, dict):
        raise SystemExit("JSON must be an object")
    return obj


async def _post_json(session, base_url: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """POST JSON using stdlib `urllib` (runner stays dependency-light)."""

    import urllib.error
    import urllib.request

    url = base_url.rstrip("/") + path
    data = json.dumps(payload).encode("utf-8")

    def _do_req() -> Dict[str, Any]:
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)

    last_err: Optional[str] = None
    for attempt in range(4):
        try:
            return await asyncio.to_thread(_do_req)
        except urllib.error.HTTPError as e:
            raw = (e.read() or b"").decode("utf-8", errors="replace")
            last_err = f"HTTP {e.code}: {raw[:300]}"
            if e.code in (429, 500, 502, 503, 504) and attempt < 3:
                await asyncio.sleep(0.25 * (2**attempt))
                continue
            return {"ok": False, "http_status": e.code, "raw": raw}
        except Exception as e:
            last_err = str(e)
            if attempt >= 3:
                raise
            await asyncio.sleep(0.25 * (2**attempt))
    raise RuntimeError(last_err or "request_failed")


def _get_ok_or_fallback(resp: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(resp, dict):
        return {"ok": False, "error": "invalid_response"}
    if resp.get("ok") is True:
        return resp
    if any(k in resp for k in ("spoken_text", "decision", "call_state", "cid")) and "ok" not in resp:
        return {"ok": True, **resp}
    return resp


def _require_ok(resp: Dict[str, Any], *, where: str) -> Dict[str, Any]:
    if not isinstance(resp, dict):
        raise RuntimeError(f"{where}: invalid response type")

    resp = _get_ok_or_fallback(resp)
    if resp.get("ok") is True:
        return resp
    status = resp.get("http_status")
    raw = (resp.get("raw") or "").strip()
    err = resp.get("error") or "request_failed"
    raise RuntimeError(f"{where}: {err} (status={status}) {raw[:500]}")


def _default_variables_for_bot(i: int, *, rng: random.Random) -> Dict[str, Any]:
    names = [
        "Sergio Figueras",
        "Camila Rojas",
    ]
    return {
        "customer_id": f"C{i:06d}",
        "fullName": rng.choice(names),
        "debtValue": f"{rng.choice([15000, 25000, 40000])} CLP",
    }


def _summarize_call_state(call_state: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k in (
        "call_id",
        "tenant_id",
        "language",
        "call_direction",
        "call_outcome",
        "dnc_requested",
        "call_should_end",
        "tool_results",
        "tool_stats",
        "billing",
        "customer",
        "ptp",
        "debt_value",
        "government_id",
    ):
        if k in call_state:
            out[k] = call_state.get(k)
    return out


def _detect_flow_issues(transcript: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Detect workflow loops/regressions from the captured transcript.

    This broadens what we consider "hallucinated" in reports: flow loops/regressions
    are a quality failure even if they aren't factual hallucinations.
    """

    assistant_turns = [e for e in (transcript or []) if (e.get("role") or "").lower() == "assistant"]
    if not assistant_turns:
        return {"hallucinated": False, "reasons": [], "evidence": [], "notes": ""}

    reasons: List[str] = []
    evidence: List[Dict[str, Any]] = []

    # 1) Repeated identical assistant text.
    text_counts: Dict[str, int] = {}
    for e in assistant_turns:
        txt = str(e.get("text") or "").strip()
        if not txt:
            continue
        text_counts[txt] = text_counts.get(txt, 0) + 1
    repeated_texts = [t for t, n in text_counts.items() if n >= 3]
    if repeated_texts:
        reasons.append("repeated_assistant_text")
        for t in repeated_texts[:3]:
            evidence.append({"assistant_quote": t[:240], "issue": "Repeated assistant message >=3 times"})

    # 2) Repeated action_type in a row.
    run_action = None
    run_len = 0
    max_run = 0
    max_run_action = None
    for e in assistant_turns:
        at = str(e.get("action_type") or "").strip()
        if at and at == run_action:
            run_len += 1
        else:
            run_action = at
            run_len = 1
        if run_len > max_run:
            max_run = run_len
            max_run_action = run_action
    if max_run_action and max_run >= 3:
        reasons.append("repeated_action_type")
        evidence.append(
            {
                "assistant_quote": str(max_run_action),
                "issue": f"Same action_type repeated {max_run} assistant turns in a row",
            }
        )

    # 3) PTP regression: re-asking for PTP date/debt after we already have amount+date.
    for idx, e in enumerate(assistant_turns):
        at = str(e.get("action_type") or "").strip().upper()
        cs = e.get("call_state") or {}
        ptp = (cs.get("ptp") or {}) if isinstance(cs, dict) else {}
        has_ptp_details = bool(ptp) and (ptp.get("amount") not in (None, "", 0)) and bool(ptp.get("due_date"))

        if has_ptp_details and at in ("INFORM_DEBT_AND_ASK_PTP", "ASK_PTP_DETAILS"):
            reasons.append("reasked_ptp_after_details_present")
            evidence.append(
                {
                    "assistant_quote": str(e.get("text") or "")[:240],
                    "issue": f"Action {at} used even though ptp.amount+ptp.due_date already present",
                    "turn_index": idx,
                }
            )
            break

    # 4) Identity mismatch accepted: asked for X, user claims to be Y, assistant proceeds.
    # Heuristic: if first assistant asked ASK_IDENTITY_CONFIRMATION containing a name,
    # a user says "soy <name>", and the next assistant discloses debt (INFORM_DEBT_AND_ASK_PTP)
    # while still addressing the original name.
    import re

    def _extract_called_name(text: str) -> str:
        t = (text or "").strip()
        m = re.search(r"hablo\s+con\s+([^?]+)\?", t, flags=re.IGNORECASE)
        return (m.group(1).strip() if m else "")

    def _extract_soy_name(text: str) -> str:
        t = (text or "").strip()
        # Capture first token after 'soy'. We will ignore pronouns like 'yo'.
        m = re.search(r"\bsoy\s+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+)", t, flags=re.IGNORECASE)
        if not m:
            return ""
        token = m.group(1).strip()
        # Ignore pronouns / non-name confirmations (natural confirmations like 'soy yo').
        if token.lower() in ("yo", "el", "ella", "usted", "ustedes", "alguien"):
            return ""
        return token

    called = ""
    for e in transcript:
        if (e.get("role") or "").lower() == "assistant" and str(e.get("action_type") or "") == "ASK_IDENTITY_CONFIRMATION":
            called = _extract_called_name(str(e.get("text") or ""))
            if called:
                break

    if called:
        for i in range(len(transcript) - 2):
            u = transcript[i]
            a = transcript[i + 1]
            if (u.get("role") or "").lower() != "user":
                continue
            soy = _extract_soy_name(str(u.get("text") or ""))
            if not soy:
                continue
            if soy.lower() == called.split()[0].lower():
                continue
            if (a.get("role") or "").lower() == "assistant" and str(a.get("action_type") or "") == "INFORM_DEBT_AND_ASK_PTP":
                reasons.append("identity_mismatch_accepted")
                evidence.append(
                    {
                        "assistant_quote": str(a.get("text") or "")[:240],
                        "issue": f"Asked for '{called}', user said 'soy {soy}', but bot proceeded to debt disclosure.",
                    }
                )
                break

    # 5) Terminal outcome missing: outbound calls should end in SUCCESS/UNSUCCESS.
    last_assistant = assistant_turns[-1] if assistant_turns else {}
    last_cs = last_assistant.get("call_state") or {}
    if isinstance(last_cs, dict):
        co = str(last_cs.get("call_outcome") or "").strip().upper()
        if co not in ("SUCCESS", "UNSUCCESS"):
            reasons.append("missing_call_outcome")
            evidence.append(
                {
                    "assistant_quote": str(last_assistant.get("text") or "")[:240],
                    "issue": "Conversation ended without call_state.call_outcome in {SUCCESS, UNSUCCESS}",
                }
            )

    hallucinated = bool(reasons)
    notes = "" if not hallucinated else "flow_issue_detected"
    return {"hallucinated": hallucinated, "reasons": reasons, "evidence": evidence, "notes": notes}


def _combine_hallucination_results(*, factual: Dict[str, Any], flow: Dict[str, Any]) -> Dict[str, Any]:
    factual_h = bool((factual or {}).get("hallucinated"))
    flow_h = bool((flow or {}).get("hallucinated"))
    combined_h = factual_h or flow_h

    reasons: List[str] = []
    for r in (factual or {}).get("reasons") or []:
        reasons.append(str(r))
    for r in (flow or {}).get("reasons") or []:
        reasons.append(f"flow:{r}")

    evidence: List[Dict[str, Any]] = []
    for ev in (factual or {}).get("evidence") or []:
        if isinstance(ev, dict):
            evidence.append(ev)
    for ev in (flow or {}).get("evidence") or []:
        if isinstance(ev, dict):
            evidence.append({"source": "flow", **ev})

    notes_parts = []
    if str((factual or {}).get("notes") or "").strip():
        notes_parts.append(str(factual.get("notes") or "").strip())
    if str((flow or {}).get("notes") or "").strip():
        notes_parts.append(str(flow.get("notes") or "").strip())

    return {
        "hallucinated": bool(combined_h),
        "reasons": reasons,
        "evidence": evidence,
        "notes": " | ".join(notes_parts),
        "factual": factual,
        "flow": flow,
    }


@dataclass
class RunnerConfig:
    base_url: str
    partner_id: str
    direction: str
    language: str
    bots: int
    concurrency: int
    max_turns: int
    out_csv: str
    seed: int
    variables: Dict[str, Any]
    extra_config: Dict[str, Any]


async def run_one_conversation(
    *,
    bot_idx: int,
    run_id: str,
    cfg: RunnerConfig,
    session,
    csv_lock: asyncio.Lock,
    writer: csv.DictWriter,
    csv_file,
) -> None:
    from toolkits.acme_inc.auto_test.scenario import build_scenario
    from utils.openai_simulation import OpenAIChatClient, evaluate_hallucination, generate_customer_message, pick_persona

    t0 = _now_ms()
    rng = random.Random(cfg.seed + bot_idx)
    cid = f"acmeinc-{run_id}-{bot_idx:06d}"

    vars_generated = _default_variables_for_bot(bot_idx, rng=rng)
    variables = {**vars_generated, **(cfg.variables or {})}

    transcript: List[Dict[str, Any]] = []
    last_action_type = "__start__"
    err: Optional[str] = None
    hallucinated: Optional[bool] = None
    analysis_json: Dict[str, Any] = {}
    persona_name = "unknown"

    try:
        scenario = build_scenario()

        # OpenAI used for simulator + factual evaluator.
        nlg = OpenAIChatClient()
        persona = pick_persona(rng=rng, scenario=scenario)

        # Runner knob: force some conversations to be adversarial.
        adv_rate = 0.0
        try:
            adv_rate = float(((cfg.extra_config or {}).get("_runner") or {}).get("adversarial_rate") or 0.0)
        except Exception:
            adv_rate = 0.0
        adv_rate = max(0.0, min(1.0, adv_rate))

        if adv_rate > 0 and rng.random() < adv_rate:
            for p in scenario.personas:
                if getattr(p, "name", "") == "adversarial_trickster":
                    persona = p
                    break
        persona_name = persona.name

        init_payload = {
            "cid": cid,
            "partnerId": cfg.partner_id,
            "text": "(init)",
            "language": cfg.language,
            "direction": cfg.direction,
            "variables": variables,
            "config": cfg.extra_config,
            "reset": True,
        }
        _require_ok(await _post_json(session, cfg.base_url, "/test/text_turn", init_payload), where="init/reset")

        out = _require_ok(
            await _post_json(
                session,
                cfg.base_url,
                "/test/text_turn",
                {
                    "cid": cid,
                    "partnerId": cfg.partner_id,
                    "direction": cfg.direction,
                    "start": True,
                    "language": cfg.language,
                },
            ),
            where="start",
        )

        loop_guard: Dict[str, int] = {}
        for _turn in range(cfg.max_turns):
            spoken = str(out.get("spoken_text") or "").strip()
            decision = out.get("decision") or {}
            call_state = out.get("call_state") or {}
            action_type = str(decision.get("action_type") or decision.get("tool_name") or "RESPOND_USER")
            last_action_type = action_type

            transcript.append(
                {
                    "role": "assistant",
                    "text": spoken,
                    "action_type": action_type,
                    "decision": decision,
                    "call_state": _summarize_call_state(call_state),
                }
            )

            if action_type in (
                "PTP_USER_REFUSED_OFFER_OPTIONS",
                "PTP_USER_UNSURE_OFFER_OPTIONS",
                "WRONG_PERSON",
                "INFORM_TECHNICAL_ISSUE",
            ):
                break

            if bool(call_state.get("call_should_end")):
                break
            if action_type in ("DO_NOT_CALL_END", "WRONG_PERSON_END"):
                break

            if spoken:
                loop_guard[spoken] = loop_guard.get(spoken, 0) + 1
                if loop_guard[spoken] >= 3:
                    break

            user_msg = await generate_customer_message(
                nlg=nlg,
                scenario=scenario,
                persona=persona,
                rng=rng,
                conversation=transcript,
                last_bot_text=spoken,
                last_action_type=action_type,
            )
            transcript.append({"role": "user", "text": user_msg})

            out = _require_ok(
                await _post_json(
                    session,
                    cfg.base_url,
                    "/test/text_turn",
                    {
                        "cid": cid,
                        "partnerId": cfg.partner_id,
                        "text": user_msg,
                        "language": cfg.language,
                        "direction": cfg.direction,
                        "variables": variables,
                        "config": cfg.extra_config,
                    },
                ),
                where="turn",
            )

        final_call_state = out.get("call_state") or {}
        ground_truth = {
            "variables": variables,
            "tool_results": (final_call_state.get("tool_results") or {}),
            "call_state_facts": {
                "debt_value": final_call_state.get("debt_value") or final_call_state.get("debtValue"),
                "customer_id": final_call_state.get("customer_id") or variables.get("customer_id"),
                "customer_name": final_call_state.get("customer_name") or variables.get("fullName"),
            },
        }

        factual_json = await evaluate_hallucination(nlg=nlg, conversation=transcript, ground_truth=ground_truth)
        flow_json = _detect_flow_issues(transcript)
        analysis_json = _combine_hallucination_results(factual=factual_json, flow=flow_json)
        hallucinated = bool(analysis_json.get("hallucinated"))

        try:
            _require_ok(
                await _post_json(
                    session,
                    cfg.base_url,
                    "/test/text_turn",
                    {"cid": cid, "partnerId": cfg.partner_id, "end": True},
                ),
                where="end",
            )
        except Exception:
            pass

    except Exception as e:
        err = str(e)

    duration_ms = _now_ms() - t0
    row = {
        "run_id": run_id,
        "bot_idx": bot_idx,
        "cid": cid,
        "partner_id": cfg.partner_id,
        "direction": cfg.direction,
        "language": cfg.language,
        "max_turns": cfg.max_turns,
        "actual_events": len(transcript),
        "duration_ms": duration_ms,
        "persona": persona_name,
        "last_action_type": last_action_type,
        "variables_json": _dumps(variables),
        "conversation_json": _dumps(transcript),
        "hallucinated": "" if hallucinated is None else str(bool(hallucinated)).lower(),
        "analysis_json": _dumps(analysis_json) if analysis_json else "{}",
        "error": err or "",
    }

    async with csv_lock:
        writer.writerow(row)
        try:
            csv_file.flush()
        except Exception:
            pass


async def run(cfg: RunnerConfig) -> int:
    out_path = Path(cfg.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "run_id",
        "bot_idx",
        "cid",
        "partner_id",
        "direction",
        "language",
        "max_turns",
        "actual_events",
        "duration_ms",
        "persona",
        "last_action_type",
        "variables_json",
        "conversation_json",
        "hallucinated",
        "analysis_json",
        "error",
    ]

    csv_lock = asyncio.Lock()
    session = None

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        sem = asyncio.Semaphore(cfg.concurrency)
        run_id = str(uuid.uuid4())[:8]

        async def _run_one(i: int):
            async with sem:
                await run_one_conversation(
                    bot_idx=i,
                    run_id=run_id,
                    cfg=cfg,
                    session=session,
                    csv_lock=csv_lock,
                    writer=writer,
                    csv_file=f,
                )

        tasks = [asyncio.create_task(_run_one(i)) for i in range(cfg.bots)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Surfacing exceptions is critical for debugging why rows might not be written.
        errs = [r for r in results if isinstance(r, BaseException)]
        if errs:
            print(f"[acme_inc_autotest] {len(errs)} task(s) raised exceptions", file=sys.stderr)
            for e in errs[:5]:
                print(f"  - {type(e).__name__}: {str(e)[:400]}", file=sys.stderr)
    return 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env-file", default=None, help="Optional env file (e.g. mock.env.local) to load for OPENAI_API_KEY")
    ap.add_argument("--base-url", default=os.getenv("CORE_BASE_URL", "http://127.0.0.1:8001"))
    ap.add_argument("--partner", "--partnerId", dest="partner_id", default="acme-inc")
    ap.add_argument("--direction", default="outbound", choices=["inbound", "outbound"])
    ap.add_argument("--language", default="pt-BR")
    ap.add_argument("--bots", type=int, default=1)
    ap.add_argument(
        "--adversarial-rate",
        type=float,
        default=0.0,
        help="Probability [0..1] that a bot will use the adversarial_trickster persona.",
    )
    ap.add_argument("--concurrency", type=int, default=50)
    ap.add_argument("--max-turns", type=int, default=20)
    ap.add_argument("--out-csv", default=None)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--variables", default=None, help="JSON object string (merged into per-bot defaults)")
    ap.add_argument("--config", default=None, help="JSON object string passed to /test/text_turn")
    args = ap.parse_args()

    env_file = _resolve_env_file(str(args.env_file) if args.env_file else None)
    if env_file:
        _load_env_file(env_file)

    if args.bots <= 0:
        raise SystemExit("--bots must be > 0")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be > 0")
    if args.max_turns <= 0:
        raise SystemExit("--max-turns must be > 0")

    out_csv = args.out_csv
    if not out_csv:
        ts = time.strftime("%Y%m%d_%H%M%S")
        out_csv = f"artifacts/acme_inc_autotest_{ts}.csv"

    cfg = RunnerConfig(
        base_url=str(args.base_url),
        partner_id=str(args.partner_id),
        direction=str(args.direction),
        language=str(args.language),
        bots=int(args.bots),
        concurrency=int(args.concurrency),
        max_turns=int(args.max_turns),
        out_csv=str(out_csv),
        seed=int(args.seed),
        variables=_parse_json_obj(args.variables),
        extra_config=_parse_json_obj(args.config),
    )

    # Store runner-only knobs into extra_config so they can be used inside run_one_conversation.
    cfg.extra_config = {**(cfg.extra_config or {}), "_runner": {"adversarial_rate": float(args.adversarial_rate)}}

    raise SystemExit(asyncio.run(run(cfg)))


if __name__ == "__main__":
    main()
