"""ACME Inc workflow (planner rules).

This contains tenant-specific planning/business rules. The system-level
orchestrator (`server/orchestrator_graph.py`) delegates planning to this object.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def _parse_money_amount(x: Any) -> Optional[float]:
    """Parse a money-ish amount from campaign debt strings.

    Examples:
      - "25000 CLP" -> 25000.0
      - "$25.000" -> 25000.0

    Conservative: returns None if no digits are present.
    """

    s = str(x or "").strip()
    if not s:
        return None

    # Keep digits only (treat separators as noise)
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return None
    try:
        return float(digits)
    except Exception:
        return None


def _campaign_debt_amount(call_state: Dict[str, Any]) -> Optional[float]:
    dv = (
        call_state.get("debt_value")
        or call_state.get("debtValue")
        or ((call_state.get("variables") or {}).get("debtValue"))
    )
    return _parse_money_amount(dv)


def _norm_name(s: Any) -> str:
    """Normalize a person name for loose matching.

    Lowercase, strip accents, collapse spaces, keep only letters/spaces.
    """

    t = _norm_text(s)
    if not t:
        return ""
    out = []
    for ch in t:
        if ch.isalpha() or ch.isspace():
            out.append(ch)
    return " ".join("".join(out).split())


def _first_token(name: str) -> str:
    parts = (name or "").split()
    return parts[0] if parts else ""


def _names_compatible(expected_name: Any, reported_name: Any) -> bool:
    """Return True if the reported name could plausibly be the expected name.

    We accept:
    - exact match
    - first name match (e.g., "Maria" vs "Maria Gonzalez")
    """

    exp = _norm_name(expected_name)
    rep = _norm_name(reported_name)
    if not exp or not rep:
        return False
    if exp == rep:
        return True
    # first name match
    return _first_token(exp) == _first_token(rep)


def _looks_like_refund_claim(user_norm: str) -> bool:
    # Spanish phrases that indicate the customer claims the company owes them money.
    # This is common adversarial bait and should NOT be interpreted as a PTP.
    return any(
        k in (user_norm or "")
        for k in (
            "me deben",
            "me tiene que pagar",
            "me tienen que pagar",
            "me van a pagar",
            "me van a depositar",
            "devolver plata",
            "devolver dinero",
            "reembolso",
            "devolucion",
            "me deben plata",
            "me deben dinero",
        )
    )


def _is_valid_ptp_amount(x: Any) -> bool:
    try:
        v = float(x)
    except Exception:
        return False
    return v > 0


def _norm_text(s: Any) -> str:
    """Cheap normalization for intent-ish matching inside the workflow.

    We keep this tiny on purpose (no extra deps). The orchestrator has a more
    robust normalizer, but the workflow needs a quick guardrail for cases where
    the NLU intent comes back as `unknown`.
    """

    t = str(s or "").strip().lower()
    if not t:
        return ""
    # minimal accent handling for common cases
    t = (
        t.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ü", "u")
        .replace("ñ", "n")
    )
    return " ".join(t.split())


def _extract_iso_date(text: str) -> Optional[str]:
    """Extract YYYY-MM-DD from free text.

    This is a tiny fallback used when NLU fails to populate entities.dates.
    """

    t = str(text or "")
    m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", t)
    return m.group(1) if m else None


def _tool_exists(available_tools: List[Dict[str, Any]], name: str) -> bool:
    return any((t.get("name") or "") == name for t in (available_tools or []))


def _first_in_list(x: Any) -> Optional[Any]:
    if isinstance(x, list) and x:
        return x[0]
    return None


def _extract_customer_id(call_state: Dict[str, Any]) -> Optional[str]:
    cid = (
        call_state.get("customer_id")
        or call_state.get("customerId")
        or call_state.get("customer")
        or call_state.get("customer_id")
    )
    if cid is None:
        return None
    cid = str(cid).strip()
    return cid or None


def _get_tool_attempts(call_state: Dict[str, Any], tool_name: str) -> int:
    stats = call_state.get("tool_stats") or {}
    per = stats.get(tool_name) or {}
    try:
        return int(per.get("attempts", 0) or 0)
    except Exception:
        return 0


def _get_tool_last_error(call_state: Dict[str, Any], tool_name: str) -> Optional[str]:
    stats = call_state.get("tool_stats") or {}
    per = stats.get(tool_name) or {}
    err = per.get("last_error")
    return str(err) if err else None


class AcmeIncWorkflow:
    """ACME Inc-specific billing/customer-care planner."""

    async def plan(self, state: Dict[str, Any]) -> Dict[str, Any]:
        call_state = state.get("call_state") or {}
        nlu = state.get("nlu") or {}
        available_tools = state.get("available_tools") or []

        intent = (nlu.get("intent") or "unknown").lower()
        entities = nlu.get("entities") or {}
        customer_id = _extract_customer_id(call_state)
        user_text = str(nlu.get("transcription") or "").strip()
        user_norm = _norm_text(user_text)

        # Extract possible customer-reported names (NLU may provide these in newer schema).
        # Keep backward compatibility: if not present, leave empty.
        person_names = entities.get("person_names")
        if isinstance(person_names, str):
            person_names = [person_names]
        if not isinstance(person_names, list):
            person_names = []
        person_names = [str(x).strip() for x in person_names if str(x).strip()]
        self_reported_name = str(entities.get("self_reported_name") or "").strip()

        # ---------- Hard stop / opt-out (Do Not Call) ----------
        # This is a compliance boundary: do NOT delegate to the LLM.
        # Mark immediately and end the interaction (caller may also hang up).
        if intent in ("do_not_call_request", "opt_out", "unsubscribe") or (
            "no me llame" in user_norm
            or "no me llames" in user_norm
            or "no llame mas" in user_norm
            or "no llamar mas" in user_norm
            or "no quiero que me llame" in user_norm
            or "no quiero que me llamen" in user_norm
            or "no me contacte" in user_norm
        ):
            call_state["dnc_requested"] = True
            call_state["call_should_end"] = True
            call_state["call_outcome"] = "UNSUCCESS"
            call_state.setdefault("compliance", {})["do_not_call"] = True
            return {
                "type": "final_answer",
                "tool_name": None,
                "tool_args": {},
                "action_type": "DO_NOT_CALL_END",
                "reason": "User requested do-not-call; mark DNC and end.",
            }

        # ---------- Outbound debt collection gate (identity confirmation) ----------
        # If the campaign provides a debt value (from CSV), we must first confirm
        # we are speaking to the correct person (by name) BEFORE disclosing any debt.
        has_campaign_debt = bool(
            call_state.get("debt_value")
            or call_state.get("debtValue")
            or ((call_state.get("variables") or {}).get("debtValue"))
        )
        if has_campaign_debt:
            dc = call_state.setdefault("debt_collection", {})
            if "expected_name" not in dc:
                dc["expected_name"] = call_state.get("customer_name") or call_state.get("fullName")

            # If we already concluded this is the wrong person, keep the conversation from looping.
            if bool(dc.get("call_should_end")):
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "WRONG_PERSON_END",
                    "reason": "Call previously marked to end due to wrong-person denial.",
                }

            # Step 1: identity confirmation
            if not bool(dc.get("identity_confirmed")):
                negative_identity_intents = {
                    "deny_knowledge",
                    "dont_know_person",
                    "not_the_person",
                }

                conf = (entities.get("confirmation") or "").lower().strip()
                is_negative = conf in ("no", "n", "false", "deny") or intent in negative_identity_intents

                expected_name = dc.get("expected_name") or call_state.get("customer_name") or call_state.get("fullName")

                # Identity mismatch detection:
                # If the customer explicitly says they are a different name, treat as wrong-person.
                reported = self_reported_name
                if not reported and person_names:
                    # Heuristic: use the first extracted name.
                    reported = person_names[0]

                if reported and expected_name and (not _names_compatible(expected_name, reported)):
                    dc["identity_confirmed"] = False
                    dc["wrong_person"] = True
                    dc["wrong_person_name_reported"] = reported
                    # Avoid looping forever asking for the same person.
                    try:
                        dc["wrong_person_attempts"] = int(dc.get("wrong_person_attempts", 0) or 0) + 1
                    except Exception:
                        dc["wrong_person_attempts"] = 1

                    # First mismatch: ask best contact time (do not disclose debt).
                    # Repeated mismatch: end.
                    call_state["call_outcome"] = "UNSUCCESS"
                    if dc.get("wrong_person_attempts", 0) >= 2:
                        dc["call_should_end"] = True
                        call_state["call_should_end"] = True
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "WRONG_PERSON_END",
                            "reason": "Customer reported a different name than expected; end after repeated mismatch.",
                        }

                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "WRONG_PERSON",
                        "reason": "Customer reported a different name than expected; do not disclose debt.",
                    }

                if conf in ("yes", "y", "true", "confirm", "confirmed"):
                    dc["identity_confirmed"] = True
                    dc["name_confirmed_at"] = "now"
                    call_state["call_outcome"] = call_state.get("call_outcome") or ""
                    # In both voice and text harness modes, the next assistant message will
                    # disclose the debt and ask for a payment date. Mark debt_disclosed now so
                    # the *next* user turn is handled as post-disclosure (avoid looping).
                    dc["debt_disclosed"] = True
                    dc["awaiting_ptp"] = True
                    # Next step: disclose debt and ask for PTP
                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "INFORM_DEBT_AND_ASK_PTP",
                        "reason": "Identity confirmed; can disclose debt and ask for promise to pay.",
                    }

                if is_negative:
                    dc["identity_confirmed"] = False
                    dc["wrong_person"] = True

                    # Avoid looping forever asking for the same person.
                    # First "no" -> WRONG_PERSON (ask best contact time).
                    # Repeated denial / explicit "I don't know that person" -> end politely.
                    try:
                        dc["wrong_person_attempts"] = int(dc.get("wrong_person_attempts", 0) or 0) + 1
                    except Exception:
                        dc["wrong_person_attempts"] = 1

                    # First denial: ask best contact time. Repeated denial: end.
                    call_state["call_outcome"] = "UNSUCCESS"
                    if dc.get("wrong_person_attempts", 0) >= 2 or intent in negative_identity_intents:
                        dc["call_should_end"] = True
                        call_state["call_should_end"] = True
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "WRONG_PERSON_END",
                            "reason": "Repeated wrong-person denial; end call politely.",
                        }

                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "WRONG_PERSON",
                        "reason": "User denied identity; do not disclose debt.",
                    }

                # Unknown / not answered yet
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "ASK_IDENTITY_CONFIRMATION",
                    "reason": "Must confirm we are speaking to the correct person before discussing debt.",
                }

            # Step 2: debt disclosure once per call (and prompt PTP)
            if not bool(dc.get("debt_disclosed")):
                dc["debt_disclosed"] = True
                # After disclosing the debt, we are effectively in a PTP conversation.
                # Track this so we can respond humanly even if NLU returns `unknown`.
                dc["awaiting_ptp"] = True
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "INFORM_DEBT_AND_ASK_PTP",
                    "reason": "Identity already confirmed; disclose debt and ask for promise to pay.",
                }

            # Step 3: post-disclosure handling (PTP-ish), but only when the user
            # isn't asking for another safe action. This prevents "technical issue"
            # fallbacks for inputs like "no sé" / "no sei".
            if bool(dc.get("identity_confirmed")) and bool(dc.get("debt_disclosed")) and bool(dc.get("awaiting_ptp")):
                # If user is clearly asking something else, let the rest of the workflow handle it.
                # (invoice intents, customer summary, etc.)
                if intent in (
                    "pay_last_bill",
                    "check_last_bill",
                    "check_last_invoice",
                    "check_last_invoice_status",
                    "fetch_customer_summary",
                    "customer_summary",
                    "check_debt",
                    "check_balance",
                    "promise_to_pay",
                    "create_promise_to_pay",
                ):
                    pass
                else:
                    # Adversarial guard: do not interpret refund/dispute claims as promise-to-pay.
                    if _looks_like_refund_claim(user_norm):
                        call_state["call_should_end"] = True
                        call_state["call_outcome"] = "UNSUCCESS"
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "PTP_USER_REFUSED_OFFER_OPTIONS",
                            "reason": "User claims refund/negative debt; do not treat as PTP.",
                        }

                    conf = (entities.get("confirmation") or "").lower().strip()
                    # NLU sometimes fails to populate `entities.confirmation` for short replies like
                    # "sí", "sí, está bien", etc. Use a conservative text fallback so the flow
                    # can still progress.
                    if not conf:
                        # Remove common punctuation so we can match short confirmations reliably.
                        un = (
                            user_norm.replace(",", "")
                            .replace(".", "")
                            .replace("!", "")
                            .replace("?", "")
                            .strip()
                        )
                        if un in (
                            "si",
                            "si correcto",
                            "si esta bien",
                            "ok",
                            "de acuerdo",
                            "dale",
                        ) or un.startswith("si "):
                            conf = "yes"
                        elif un in (
                            "no",
                            "no gracias",
                            "no esta bien",
                        ) or un.startswith("no "):
                            conf = "no"
                    amt = _first_in_list(entities.get("amounts"))
                    dt = _first_in_list(entities.get("dates"))
                    if not dt:
                        # Fallback: accept explicit ISO dates even if NLU missed it.
                        dt = _extract_iso_date(user_text)

                    # IMPORTANT: confirmation handling inside the debt-collection PTP stage.
                    # In outbound collections, NLU may label a plain "sí" after CONFIRM_PTP as intent=unknown.
                    # If we already have amount + date in call_state['ptp'], treat this as confirmation and
                    # proceed to create the promise-to-pay tool, instead of re-asking the debt/date.
                    ptp = call_state.setdefault("ptp", {})

                    # ACME Inc outbound collections: PTP is *date-only*.
                    # We always register the promise with the full campaign debt amount.
                    campaign_amt = _campaign_debt_amount(call_state)

                    # Date confirmation: accept either an explicit yes OR the user repeating
                    # the same date again.
                    date_confirmed = False
                    if ptp.get("awaiting_date_confirmation") and ptp.get("due_date"):
                        if conf in ("yes", "y", "true", "confirm", "confirmed"):
                            date_confirmed = True
                        # If the user repeats the same date, treat that as confirmation.
                        if (not date_confirmed) and dt and str(dt) == str(ptp.get("due_date")):
                            date_confirmed = True

                    # If user provided a date:
                    # - if we're already awaiting confirmation and they repeated the same date,
                    #   it will be handled above as confirmation.
                    # - otherwise, treat it as setting/updating the date and ask for confirmation.
                    if dt and (not date_confirmed):
                        ptp["due_date"] = str(dt)
                        if campaign_amt is not None and campaign_amt > 0:
                            ptp["amount"] = float(campaign_amt)
                        ptp["awaiting_date_confirmation"] = True
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "CONFIRM_PTP_DATE",
                            "reason": "User provided payment date; confirm date before creating PTP.",
                        }

                    if date_confirmed and ptp.get("due_date"):
                        # Ensure amount is set to full campaign debt (best-effort).
                        if ptp.get("amount") in (None, "", 0) and campaign_amt is not None and campaign_amt > 0:
                            ptp["amount"] = float(campaign_amt)

                        # Guard: do not allow invalid/negative amounts even if confirmed.
                        if not _is_valid_ptp_amount(ptp.get("amount")):
                            ptp["amount"] = None
                            return {
                                "type": "final_answer",
                                "tool_name": None,
                                "tool_args": {},
                                "action_type": "ASK_PTP_DETAILS",
                                "missing_fields": ["due_date"],
                                "reason": "Invalid campaign debt amount; request a payment date again.",
                            }
                        ptp["confirmed"] = True
                        ptp["awaiting_date_confirmation"] = False

                        missing = []
                        if not customer_id:
                            missing.append("customer_id")
                        if not ptp.get("due_date"):
                            missing.append("due_date")
                        if missing:
                            return {
                                "type": "final_answer",
                                "tool_name": None,
                                "tool_args": {},
                                "action_type": "ASK_PTP_DETAILS",
                                "missing_fields": list(missing),
                                "reason": "User confirmed PTP, but required fields are missing.",
                            }

                        tool_name = "create_promise_to_pay"
                        if not _tool_exists(available_tools, tool_name):
                            call_state["call_should_end"] = True
                            call_state["call_outcome"] = "UNSUCCESS"
                            return {
                                "type": "final_answer",
                                "tool_name": None,
                                "tool_args": {},
                                "action_type": "INFORM_TECHNICAL_ISSUE",
                                "reason": "PTP tool not available for this tenant.",
                            }

                        attempts = _get_tool_attempts(call_state, tool_name)
                        last_err = _get_tool_last_error(call_state, tool_name)
                        if last_err and attempts >= 2:
                            call_state["call_should_end"] = True
                            call_state["call_outcome"] = "UNSUCCESS"
                            return {
                                "type": "final_answer",
                                "tool_name": None,
                                "tool_args": {},
                                "action_type": "INFORM_TECHNICAL_ISSUE",
                                "reason": f"Tool '{tool_name}' failed multiple times; not retrying.",
                            }

                        return {
                            "type": "tool",
                            "tool_name": tool_name,
                            "tool_args": {
                                "customer_id": customer_id,
                                "amount": ptp.get("amount"),
                                "due_date": ptp.get("due_date"),
                            },
                            "action_type": "CREATE_PTP_AND_CONFIRM" if not last_err else "RETRY_TOOL",
                            "reason": "User confirmed PTP; creating promise to pay via tenant tool.",
                        }

                    if conf in ("no", "n", "false", "deny") and ptp.get("due_date"):
                        ptp["confirmed"] = False
                        ptp["awaiting_date_confirmation"] = False
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "ASK_PTP_DETAILS",
                            "reason": "User denied the proposed PTP; request updated amount/date.",
                        }

                    # If the user provides any PTP-ish data, treat as progress.
                    # In this tenant's outbound debt flow, we use *only* the date and
                    # force the amount to the full campaign debt.
                    if amt is not None or dt:
                        if dt:
                            ptp["due_date"] = str(dt)
                        if campaign_amt is not None and campaign_amt > 0:
                            ptp["amount"] = float(campaign_amt)

                        missing = []
                        if not customer_id:
                            missing.append("customer_id")
                        if not ptp.get("due_date"):
                            missing.append("due_date")

                        if missing:
                            return {
                                "type": "final_answer",
                                "tool_name": None,
                                "tool_args": {},
                                "action_type": "ASK_PTP_DETAILS",
                                "missing_fields": list(missing),
                                "reason": "User provided partial PTP info (implicit); need remaining fields.",
                            }

                        ptp["awaiting_date_confirmation"] = True
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "CONFIRM_PTP_DATE",
                            "reason": "Payment date present; confirm date before creating promise.",
                        }

                    # Prefer user's preference C: do not insist. Offer options quickly.
                    is_refuse = conf in ("no", "n", "false", "deny") or user_norm in ("no", "nop", "nope")
                    is_unsure = conf in ("unsure", "maybe", "not_sure") or user_norm in (
                        "no se",
                        "nose",
                        "no sei",
                        "no estoy seguro",
                        "no estoy segura",
                        "no tengo idea",
                    )

                    if is_refuse:
                        dc["ptp_options_offered"] = True
                        call_state["call_should_end"] = True
                        call_state["call_outcome"] = "UNSUCCESS"
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "PTP_USER_REFUSED_OFFER_OPTIONS",
                            "reason": "User refused to commit; offer alternatives without insisting.",
                        }

                    if is_unsure:
                        dc["ptp_options_offered"] = True
                        call_state["call_should_end"] = True
                        call_state["call_outcome"] = "UNSUCCESS"
                        return {
                            "type": "final_answer",
                            "tool_name": None,
                            "tool_args": {},
                            "action_type": "PTP_USER_UNSURE_OFFER_OPTIONS",
                            "reason": "User unsure about payment date; offer alternatives without insisting.",
                        }

                    # Default steering inside PTP stage: if we're still awaiting a payment date
                    # and the user said something else (ordinary question, objection, etc.),
                    # keep the call in the same stage so the orchestrator can answer+anchor.
                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "INFORM_DEBT_AND_ASK_PTP",
                        "reason": "Still awaiting payment date; re-ask anchor / allow LLM steering.",
                    }

        # ---------- PTP flow ----------
        if intent in ("promise_to_pay", "create_promise_to_pay"):
            ptp = call_state.setdefault("ptp", {})
            amt = _first_in_list(entities.get("amounts"))
            dt = _first_in_list(entities.get("dates"))
            # Guard: user is disputing/refund-baiting; do not treat extracted amounts as PTP.
            if _looks_like_refund_claim(user_norm):
                call_state["call_should_end"] = True
                call_state["call_outcome"] = "UNSUCCESS"
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "PTP_USER_REFUSED_OFFER_OPTIONS",
                    "reason": "User claims refund/negative debt; do not treat as PTP.",
                }
            if amt is not None:
                try:
                    ptp["amount"] = float(amt)
                except Exception:
                    ptp["amount"] = amt
            if dt:
                ptp["due_date"] = str(dt)

            if ptp.get("amount") not in (None, "", 0) and (not _is_valid_ptp_amount(ptp.get("amount"))):
                ptp["amount"] = None
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "ASK_PTP_DETAILS",
                    "missing_fields": ["amount"],
                    "reason": "Invalid PTP amount (must be > 0).",
                }

            conf = (entities.get("confirmation") or "").lower().strip()
            if conf in ("yes", "y", "true", "confirm", "confirmed"):
                ptp["confirmed"] = True
            elif conf in ("no", "n", "false", "deny"):
                ptp["confirmed"] = False

            missing = []
            if not customer_id:
                missing.append("customer_id")
            if ptp.get("amount") in (None, "", 0):
                missing.append("amount")
            if not ptp.get("due_date"):
                missing.append("due_date")

            if missing:
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "ASK_PTP_DETAILS",
                    "missing_fields": list(missing),
                    "reason": f"PTP requested but missing: {', '.join(missing)}",
                }

            if not bool(ptp.get("confirmed")):
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "CONFIRM_PTP",
                    "reason": "PTP data present; need explicit confirmation before creating promise.",
                }

            tool_name = "create_promise_to_pay"
            if not _tool_exists(available_tools, tool_name):
                call_state["call_should_end"] = True
                call_state["call_outcome"] = "UNSUCCESS"
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "INFORM_TECHNICAL_ISSUE",
                    "reason": "PTP tool not available for this tenant.",
                }

            attempts = _get_tool_attempts(call_state, tool_name)
            last_err = _get_tool_last_error(call_state, tool_name)
            if last_err and attempts >= 2:
                call_state["call_should_end"] = True
                call_state["call_outcome"] = "UNSUCCESS"
                return {
                    "type": "final_answer",
                    "tool_name": None,
                    "tool_args": {},
                    "action_type": "INFORM_TECHNICAL_ISSUE",
                    "reason": f"Tool '{tool_name}' failed multiple times; not retrying.",
                }

            return {
                "type": "tool",
                "tool_name": tool_name,
                "tool_args": {
                    "customer_id": customer_id,
                    "amount": ptp.get("amount"),
                    "due_date": ptp.get("due_date"),
                },
                "action_type": "CREATE_PTP_AND_CONFIRM" if not last_err else "RETRY_TOOL",
                "reason": "Creating promise to pay via tenant tool.",
            }

        # ---------- Invoice intents ----------
        if intent in ("pay_last_bill", "check_last_bill", "check_last_invoice", "check_last_invoice_status"):
            tool_name = "fetch_last_invoice"
            if _tool_exists(available_tools, tool_name):
                if not customer_id:
                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "ASK_CUSTOMER_ID",
                        "reason": "Need customer identifier to fetch last invoice.",
                    }

                attempts = _get_tool_attempts(call_state, tool_name)
                last_err = _get_tool_last_error(call_state, tool_name)
                if last_err and attempts >= 2:
                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "INFORM_TECHNICAL_ISSUE",
                        "reason": f"Tool '{tool_name}' failed multiple times; not retrying.",
                    }

                return {
                    "type": "tool",
                    "tool_name": tool_name,
                    "tool_args": {"customer_id": customer_id},
                    "action_type": "CHECK_LAST_INVOICE" if not last_err else "RETRY_TOOL",
                    "reason": "User asked about last invoice; fetch via tool.",
                }

        # ---------- Customer summary / debt intents ----------
        if intent in ("fetch_customer_summary", "customer_summary", "check_debt", "check_balance"):
            tool_name = "fetch_customer_summary"
            if _tool_exists(available_tools, tool_name):
                if not customer_id:
                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "ASK_CUSTOMER_ID",
                        "reason": "Need customer identifier to fetch customer summary.",
                    }

                attempts = _get_tool_attempts(call_state, tool_name)
                last_err = _get_tool_last_error(call_state, tool_name)
                if last_err and attempts >= 2:
                    return {
                        "type": "final_answer",
                        "tool_name": None,
                        "tool_args": {},
                        "action_type": "INFORM_TECHNICAL_ISSUE",
                        "reason": f"Tool '{tool_name}' failed multiple times; not retrying.",
                    }

                return {
                    "type": "tool",
                    "tool_name": tool_name,
                    "tool_args": {"customer_id": customer_id},
                    "action_type": "FETCH_CUSTOMER_SUMMARY" if not last_err else "RETRY_TOOL",
                    "reason": "User asked about account summary; fetch via tool.",
                }

        # ---------- Fallback ----------
        return {
            "type": "final_answer",
            "tool_name": None,
            "tool_args": {},
            "action_type": "RESPOND_USER",
            "reason": f"No rule matched for intent={intent}",
        }
