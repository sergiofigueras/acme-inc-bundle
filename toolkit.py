from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict

from toolkits.base import BaseToolkit, ToolkitError

logger = logging.getLogger("tenant-toolkits")


@dataclass
class AcmeIncToolkit(BaseToolkit):
    partner_id: str = "acme-inc"

    # Default tenant configuration (language, system prompt, etc.)
    DEFAULT_CONFIG: Dict[str, Any] = field(
        default_factory=lambda: {
            "llm": {
                "system_message": (
                    "Eres un agente virtual de ACME Inc. "
                    "Debes responder SIEMPRE en español de Chile (es-CL), con un tono cordial y profesional. "
                    "Tu misión es ayudar al cliente con boletas/facturas, pagos y servicios móviles/hogar. "
                    "En llamadas salientes de cobranza, DEBES primero confirmar la identidad (nombre) "
                    "antes de mencionar cualquier monto de deuda. "
                    "Humanización (muy importante): responde de forma natural, como un humano en un call center, "
                    "sin sonar robótico ni repetir frases idénticas. Mantén las respuestas cortas y claras. "
                    "Si el cliente hace small talk (por ejemplo, clima o '¿cómo estás?'), responde con empatía y "
                    "una limitación honesta (por ejemplo: no tienes acceso en tiempo real / eres una IA), y luego "
                    "redirige suavemente al objetivo de la llamada. "
                    "Si el cliente hace una pregunta fuera de tema, contesta brevemente si corresponde y vuelve al foco. "
                    "Si el cliente es grosero, establece un límite con respeto (pide mantener el respeto) y continúa. "
                    "Evita términos demasiado técnicos; usa conectores naturales (por ejemplo: 'entiendo', 'perfecto', 'claro'). "
                    "Nunca inventes datos: si falta información o no estás seguro, pide confirmación al cliente "
                    "o explica que no tienes acceso a ese dato."
                ),
                "sampling": {
                    "temperature": 0.3,
                    "top_p": 0.9,
                    "max_tokens": 256,
                },
            },
            "languages": {
                "default": "es-CL",
                "supported": ["es-CL"],
                "force_default_reply": True,
            },
            "inactivity": {
                # Tenant-configurable phrases for inactivity handling.
                # Used by VoiceAgent (real calls) and by the text CLI (mock mode).
                "are_you_there": "¿Sigue ahí? ¿Me escucha?",
                "ending_no_response": "Como no recibo respuesta, finalizaré la llamada.",
            },
            "dialogue": {
                # Tenant-configurable dialogue templates for deterministic flows.
                # These are used by server/orchestrator_graph.py when it emits structured
                # action_types (identity gate, wrong person, etc.)
                "enabled": True,
                # attract the user back to the call objective even when
                # they ask unrelated questions.
                "steering": {"enabled": True, "mode": "attract"},
                # Identity (only reveal agent name when customer asks who is speaking)
                "agent_name": "Lana",
                "organization": "Telefónica Chile",
                "templates": {
                    # Default identity confirmation (no self-intro)
                    "confirm_identity": "Hola, ¿hablo con {name}?",
                    # Only when user asks “who is this?”
                    "confirm_identity_reveal": "Hola, soy {agent_name} de {organization}. ¿Hablo con {name}?",
                    # Contextual redirects (keep focus; do not disclose debt before confirmation)
                    "reason_for_call_then_confirm": (
                        "Entiendo. Te llamo de {organization} por un asunto relacionado con tu cuenta. "
                        "Antes de continuar, por seguridad necesito confirmar: ¿hablo con {name}?"
                    ),
                    "where_got_number_then_confirm": (
                        "Tu número está registrado en nuestros sistemas de {organization}. "
                        "Antes de darte más detalles, por seguridad necesito confirmar: ¿hablo con {name}?"
                    ),
                    "smalltalk_then_confirm": (
                        "No tengo acceso a esa información en tiempo real porque soy una IA, "
                        "pero ojalá esté agradable. De todos modos, ¿hablo con {name}?"
                    ),
                    "rude_then_confirm": (
                        "Te pido por favor que mantengamos el respeto para poder ayudarte. "
                        "¿Hablo con {name}?"
                    ),
                    "identity_and_rude_then_confirm": (
                        "Hola, soy {agent_name} de {organization}. "
                        "Te pido por favor que mantengamos el respeto para poder ayudarte. "
                        "¿Hablo con {name}?"
                    ),
                    "offtopic_then_confirm": (
                        "Entiendo. No tengo acceso a esa información en este momento. "
                        "De todos modos, ¿hablo con {name}?"
                    ),

                    # ---------- Generic wrappers (can be used for any action) ----------
                    "redirect_rude": "Te pido por favor que mantengamos el respeto. {next}",
                    "redirect_smalltalk": (
                        "Entiendo. No tengo acceso a esa información en tiempo real porque soy una IA. {next}"
                    ),
                    "redirect_offtopic": "Entiendo. {next}",

                    # Anchor questions (used by the steering layer to pull the user back to the flow)
                    # Keep these short and stable.
                    "anchor_identity": "¿Hablo con {name}?",
                    "anchor_ptp_date": [
                        "¿En qué fecha podrías realizar el pago?",
                        "¿Para qué fecha podrías realizar el pago?",
                    ],

                    # ---------- Other action_type templates (final_answer path) ----------
                    # Use list variants to reduce repetition deterministically.
                    "WRONG_PERSON": [
                        "Entiendo, disculpa la molestia. ¿Me podrías indicar cuándo puedo contactar a {name}?",
                        "Perfecto, gracias. Para no incomodar, ¿cuándo puedo ubicar a {name}?",
                    ],
                    "WRONG_PERSON_END": [
                        "Entiendo, gracias por avisar. Disculpa la molestia; no te quito más tiempo. Que tengas un buen día.",
                        "Perfecto, gracias. Disculpa la molestia. Voy a dejarlo registrado y finalizo por acá. Buen día.",
                    ],
                    "DO_NOT_CALL_END": [
                        "Entiendo. Dejo registrada tu solicitud y no te contactaremos nuevamente. Que tengas un buen día.",
                        "Perfecto, lo dejo registrado para no volver a llamarte. Gracias por tu tiempo. Buen día.",
                    ],
                    "INFORM_DEBT_AND_ASK_PTP": [
                        "Gracias, {name}. Te llamo de {organization}. Según nuestros registros, tienes un saldo pendiente de {amount}. ¿Cuándo podrías realizar el pago?",
                        "Perfecto, {name}. Te contacto de {organization}. Figura un saldo pendiente de {amount}. ¿En qué fecha podrías pagarlo?",
                    ],
                    "ASK_CUSTOMER_ID": [
                        "Para ayudarte, ¿me confirmas tu RUT o tu identificador de cliente?",
                        "Perfecto. Para revisar tu cuenta, ¿me indicas tu RUT o tu ID de cliente?",
                    ],
                    "ASK_PTP_DETAILS": [
                        "De acuerdo. Para registrarlo, ¿me confirmas la fecha de pago?",
                        "Perfecto. ¿En qué fecha vas a realizar el pago?",
                    ],
                    "CONFIRM_PTP": [
                        "Solo para confirmar: ¿vas a pagar {ptp_amount} el {ptp_due_date}?",
                        "Confirmo: {ptp_amount} para el {ptp_due_date}. ¿Está correcto?",
                    ],
                    "CONFIRM_PTP_DATE": [
                        "Solo para confirmar: ¿vas a pagar el {ptp_due_date}?",
                        "Confirmo: pagarías el {ptp_due_date}. ¿Está correcto?",
                    ],

                    # ---------- Tool outcomes (avoid LLM hallucination) ----------
                    # This action_type is emitted by the workflow when executing `create_promise_to_pay`.
                    # Use deterministic templates so we don't say "problema técnico" after success.
                    "CREATE_PTP_AND_CONFIRM": [
                        "Listo, dejé registrada tu promesa de pago por {ptp_amount} para el {ptp_due_date}. Gracias por tu tiempo. Que tengas un buen día.",
                        "Perfecto. Quedó registrada tu promesa de pago de {ptp_amount} para el {ptp_due_date}. Muchas gracias. Buen día.",
                    ],

                    # ---------- Post-disclosure PTP handling (human, low-pressure) ----------
                    "PTP_USER_UNSURE_OFFER_OPTIONS": [
                        "Entiendo. Si aún no tienes clara la fecha, no te preocupes. Si quieres, puedes pagar por la app Mi Movistar, web o en sucursal. Gracias por tu tiempo. Buen día.",
                        "Ya, entiendo. Si no sabes la fecha exacta, puedes revisar y pagar por Mi Movistar o en sucursal. Gracias por tu tiempo. Buen día.",
                    ],
                    "PTP_USER_REFUSED_OFFER_OPTIONS": [
                        "Entiendo. Igual es importante mantener tus cuentas al día para evitar cortes o recargos. Puedes pagar por Mi Movistar, web o en sucursal. Gracias por tu tiempo. Buen día.",
                        "Perfecto, entiendo. Para que lo tengas bajo control, puedes pagar por la app Mi Movistar, web o en sucursal. Gracias por tu tiempo. Buen día.",
                    ],
                    "INFORM_TECHNICAL_ISSUE": [
                        "Perdón, estoy con una inestabilidad técnica y no puedo completar la gestión ahora. ¿Te parece si lo intentamos más tarde?",
                        "Disculpa, tengo un problema técnico en este momento. Podemos intentarlo en unos minutos o si prefieres, te derivo con un agente humano.",
                    ],
                },
                # Regex patterns (matched on a normalized, accent-stripped, lowercase text)
                "patterns": {
                    # “Who is speaking / who wants to know?”
                    "identity_question": [
                        r"\bquien\s+habla\b",
                        r"\bcon\s+quien\s+hablo\b",
                        r"\bquien\s+es\b",
                        r"\bquien\s+llama\b",
                        r"\bde\s+quien\b",
                        r"\bquem\s+fala\b",
                        r"\bquem\s+quer\s+saber\b",
                        r"\bwho\s+is\s+this\b",
                        r"\bwho\s+is\s+speaking\b",
                    ],
                    # “Why are you calling / what is this about?”
                    "reason_for_call": [
                        r"\bpor\s+que\s+me\s+llamas\b",
                        r"\bpor\s+que\s+llamas\b",
                        r"\bpor\s+que\s+me\s+llaman\b",
                        r"\bpor\s+que\s+me\s+est(a|as)\s+llamando\b",
                        r"\bde\s+que\s+se\s+trata\b",
                        r"\bque\s+pasa\b",
                        r"\bqual\s+o\s+motivo\b",
                        r"\bpor\s+que\s+voce\s+me\s+lig(a|ou)\b",
                        r"\bpor\s+que\s+esta\s+ligando\b",
                    ],
                    # “How did you get my number / privacy?”
                    "where_got_number": [
                        r"\bde\s+donde\s+(sacaste|tienes)\s+mi\s+numero\b",
                        r"\bcomo\s+(consiguieron|conseguiste)\s+mi\s+numero\b",
                        r"\bcomo\s+conseguiste\s+mi\s+telefono\b",
                        r"\bdonde\s+conseguiste\s+mi\s+numero\b",
                        r"\bde\s+onde\s+voce\s+tem\s+meu\s+numero\b",
                    ],
                    # Small talk
                    "smalltalk": [
                        r"\bclima\b",
                        r"\btempo\b",
                        r"\bweather\b",
                        r"\bcomo\s+estas\b",
                        r"\bcomo\s+vai\b",
                        r"\bque\s+tal\b",
                    ],
                    # Rude / insults (minimal list; can be extended per tenant)
                    "rude": [
                        r"\bpendeja\b",
                        r"\bculo\b",
                        r"\bcul(o|a)\b",
                        r"\btamano\b",
                        r"\btamaño\b",
                        r"\bputa\b",
                        r"\bcaralho\b",
                        r"\bidiota\b",
                        r"\bestupido\b",
                        r"\bimbecil\b",
                    ],
                },
            },
        }
    )

    # Tool definitions exposed to Orchestrator
    TOOL_SPECS: Dict[str, Dict[str, Any]] = field(
        default_factory=lambda: {
            "fetch_customer_summary": {
                "description": (
                    "Fetch the customer's summary (segment, products, debt status) "
                    "from an internal identifier such as RUT or MSISDN."
                ),
                "args_schema": {
                    "type": "object",
                    "properties": {
                        "customer_id": {
                            "type": "string",
                            "description": "Internal customer identifier (for example, RUT or a BSS id).",
                        },
                    },
                    "required": ["customer_id"],
                },
            },
            "fetch_last_invoice": {
                "description": (
                    "Fetch the customer's latest invoice, including total amount, due date, and payment status."
                ),
                "args_schema": {
                    "type": "object",
                    "properties": {
                        "customer_id": {
                            "type": "string",
                            "description": "Internal customer identifier (same one used in fetch_customer_summary).",
                        },
                    },
                    "required": ["customer_id"],
                },
            },
            "create_promise_to_pay": {
                "description": (
                    "Create a promise-to-pay for the customer's current debt. "
                    "Use only when the customer explicitly confirms they want to commit to paying."
                ),
                "args_schema": {
                    "type": "object",
                    "properties": {
                        "customer_id": {"type": "string", "description": "Internal customer identifier."},
                        "amount": {"type": "number", "description": "Amount the customer commits to pay (in CLP)."},
                        "due_date": {"type": "string", "description": "ISO date (YYYY-MM-DD) when the customer will pay."},
                    },
                    "required": ["customer_id", "amount", "due_date"],
                },
            },
        }
    )

    async def tool_fetch_customer_summary(self, customer_id: str, call_state: Dict[str, Any]) -> Dict[str, Any]:
        base_url = os.getenv("ACME_INC_API_BASE_URL", "").rstrip("/")
        if not base_url:
            logger.warning("ACME_INC_API_BASE_URL not configured; returning mock summary.")
            summary = {
                "customer_id": customer_id,
                "segment": "RESIDENCIAL",
                "has_debt": False,
                "total_debt": 0,
                "status": "OK",
                "mock": True,
            }
            call_state.setdefault("customer", {})["summary"] = summary
            return summary

        url = f"{base_url}/customers/{customer_id}/summary"
        headers = {"Accept": "application/json"}
        api_key = os.getenv("ACME_INC_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        session = await self._get_session()
        async with session.get(url, headers=headers) as resp:
            if resp.status == 404:
                summary = {
                    "customer_id": customer_id,
                    "segment": None,
                    "has_debt": False,
                    "total_debt": 0,
                    "status": "NOT_FOUND",
                }
                call_state.setdefault("customer", {})["summary"] = summary
                return summary

            resp.raise_for_status()
            data = await resp.json()

        summary = {
            "customer_id": customer_id,
            "segment": data.get("segment") or data.get("customerSegment"),
            "has_debt": bool(data.get("hasDebt", False)),
            "total_debt": float(data.get("totalDebt", 0) or 0),
            "status": "OK",
            "raw": data,
        }
        call_state.setdefault("customer", {})["summary"] = summary
        return summary

    async def tool_fetch_last_invoice(self, customer_id: str, call_state: Dict[str, Any]) -> Dict[str, Any]:
        base_url = os.getenv("ACME_INC_API_BASE_URL", "").rstrip("/")
        if not base_url:
            logger.warning("ACME_INC_API_BASE_URL not configured; returning mock invoice.")
            invoice = {
                "customer_id": customer_id,
                "invoice_id": "MOCK-0001",
                "amount": 25000,
                "currency": "CLP",
                "due_date": "2024-12-31",
                "status": "PENDING",
                "mock": True,
            }
            call_state.setdefault("billing", {})["last_invoice"] = invoice
            return invoice

        url = f"{base_url}/customers/{customer_id}/invoices"
        headers = {"Accept": "application/json"}
        api_key = os.getenv("ACME_INC_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        params = {"limit": "1", "sort": "dueDate:desc"}
        session = await self._get_session()
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 404:
                invoice = {
                    "customer_id": customer_id,
                    "invoice_id": None,
                    "amount": 0,
                    "currency": "CLP",
                    "due_date": None,
                    "status": "NO_INVOICE",
                }
                call_state.setdefault("billing", {})["last_invoice"] = invoice
                return invoice

            resp.raise_for_status()
            data = await resp.json()

        if isinstance(data, list):
            raw_inv = data[0] if data else {}
        else:
            items = data.get("items") if isinstance(data, dict) else None
            raw_inv = (items or [None])[0] or {}

        invoice = {
            "customer_id": customer_id,
            "invoice_id": raw_inv.get("id") or raw_inv.get("invoiceId"),
            "amount": float(raw_inv.get("amount", 0) or 0),
            "currency": raw_inv.get("currency", "CLP"),
            "due_date": raw_inv.get("dueDate"),
            "status": raw_inv.get("status", "UNKNOWN"),
            "raw": raw_inv,
        }
        call_state.setdefault("billing", {})["last_invoice"] = invoice
        return invoice

    async def tool_create_promise_to_pay(
        self,
        customer_id: str,
        amount: float,
        due_date: str,
        call_state: Dict[str, Any],
    ) -> Dict[str, Any]:
        if amount <= 0:
            raise ToolkitError("amount must be greater than 0 to create a promise to pay.")

        base_url = os.getenv("ACME_INC_API_BASE_URL", "").rstrip("/")
        if not base_url:
            logger.warning("ACME_INC_API_BASE_URL not configured; returning mock promise.")
            promise = {
                "customer_id": customer_id,
                "promise_id": "MOCK-PROMISE-1",
                "amount": float(amount),
                "due_date": due_date,
                "status": "CREATED_MOCK",
            }
            call_state.setdefault("billing", {})["promise_to_pay"] = promise
            # For outbound collections: after registering a PTP, we should end the call.
            call_state["call_should_end"] = True
            call_state["call_outcome"] = "SUCCESS"
            return promise

        url = f"{base_url}/customers/{customer_id}/promises"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        api_key = os.getenv("ACME_INC_API_KEY")
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        payload = {
            "amount": float(amount),
            "currency": "CLP",
            "dueDate": due_date,
            "channel": "VOICE_AI",
            "metadata": {"source": "quake-slm-orchestrator"},
        }

        session = await self._get_session()
        async with session.post(url, headers=headers, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()

        promise = {
            "customer_id": customer_id,
            "promise_id": data.get("id") or data.get("promiseId"),
            "amount": float(data.get("amount", amount) or amount),
            "due_date": data.get("dueDate", due_date),
            "status": data.get("status", "CREATED"),
            "raw": data,
        }
        call_state.setdefault("billing", {})["promise_to_pay"] = promise
        call_state["call_should_end"] = True
        call_state["call_outcome"] = "SUCCESS"
        return promise
