# -*- coding: utf-8 -*-
"""الخادم المحلي لأداة الثقافة المالية وQureo.

يعتمد على مكتبات Python القياسية، ويشغّل محركات الحل الموجودة دون تغيير
واجهاتها. كل حساب يُدار في خيط مستقل مع حدّ للتوازي وسجلّ وتقدّم منفصلين.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

_ORIGINAL_STDOUT = sys.stdout

PORT_BASE = 8700
VERSION = 2
MAX_ACCOUNTS = 20
MAX_LOG_LINES = 800
MAX_REQUEST_BYTES = 64 * 1024
DEFAULT_CONCURRENCY = 4
SUPPORTED_MODES = frozenset({"spx", "qureo", "qureo2"})
QUREO_BANK_PATH = os.path.join(APP_DIR, "qureo_answers.json")
PORTAL_LOGIN = "https://me-portal.qureo.education/oauth2/password-login"
PORTAL_USERINFO = "https://me-portal.qureo.education/oauth2/userinfo"

_state_lock = threading.RLock()
_stop_all = threading.Event()
_accounts: list[dict] = []
_thread_context = threading.local()

_concurrency_condition = threading.Condition()
_current_concurrency = DEFAULT_CONCURRENCY
_active_workers = 0

_spx_module = None
_qureo_module = None
_spx_bank = None
_qureo_answers = None
_qureo_installed = False
_import_lock = threading.RLock()
_spx_bank_lock = threading.Lock()
_qureo_answers_lock = threading.RLock()


# ---------------------------------------------------------------------------
# Общие служебные функции


def _set_concurrency(value: object) -> int:
    """ضبط عدد العمليات المتزامنة ضمن النطاق المدعوم."""
    global _current_concurrency
    try:
        requested = int(value)
    except (TypeError, ValueError):
        requested = DEFAULT_CONCURRENCY

    with _concurrency_condition:
        _current_concurrency = max(1, min(12, requested))
        _concurrency_condition.notify_all()
        return _current_concurrency


def _is_stopped(account: dict | None = None) -> bool:
    if _stop_all.is_set():
        return True
    if account is None:
        account = getattr(_thread_context, "account", None)
    stop_event = account.get("_stop_event") if account else None
    return bool(stop_event and stop_event.is_set())


def _acquire_worker_slot(account: dict) -> bool:
    global _active_workers
    stop_event = account.get("_stop_event")

    with _concurrency_condition:
        while _active_workers >= _current_concurrency:
            if _stop_all.is_set() or (stop_event and stop_event.is_set()):
                return False
            _concurrency_condition.wait(timeout=0.3)

        if _stop_all.is_set() or (stop_event and stop_event.is_set()):
            return False

        _active_workers += 1
        return True


def _release_worker_slot() -> None:
    global _active_workers
    with _concurrency_condition:
        _active_workers = max(0, _active_workers - 1)
        _concurrency_condition.notify_all()


def _new_account(code: str, mode: str, detail: str) -> dict:
    return {
        "id": f"{code}_{time.time_ns() // 1_000_000}",
        "code": code,
        "mode": mode,
        "detail": detail,
        "running": True,
        "finished": False,
        "success": False,
        "message": "",
        "phase": "في الانتظار...",
        "progress": {"done": 0, "total": 0, "pct": 0, "title": ""},
        "lines": [],
        "_stop_event": threading.Event(),
        "_thread": None,
    }


def _log(account: dict, message: object, level: str = "info") -> None:
    line = {
        "t": time.strftime("%H:%M:%S"),
        "level": level,
        "msg": str(message)[:500],
    }
    with _state_lock:
        lines = account["lines"]
        lines.append(line)
        overflow = len(lines) - MAX_LOG_LINES
        if overflow > 0:
            del lines[:overflow]


def _set_progress(account: dict, done: object, total: object, title: object) -> None:
    try:
        done_value = max(0, int(done or 0))
    except (TypeError, ValueError):
        done_value = 0
    try:
        total_value = max(0, int(total or 0))
    except (TypeError, ValueError):
        total_value = 0

    progress = {
        "done": done_value,
        "total": total_value,
        "pct": round(done_value / total_value * 100) if total_value else 0,
        "title": str(title or "")[:80],
    }
    with _state_lock:
        account["progress"] = progress


def _set_phase(account: dict, phase: object) -> None:
    with _state_lock:
        account["phase"] = str(phase)


class _StdoutSink:
    """توجيه مخرجات print الصادرة من محرك Qureo إلى حساب الخيط الحالي."""

    encoding = "utf-8"

    def __init__(self) -> None:
        self._buffers: dict[int, str] = {}
        self._lock = threading.Lock()

    def reconfigure(self, **_kwargs) -> None:
        return None

    def write(self, value: str) -> int:
        if not value:
            return 0
        thread_id = threading.get_ident()
        with self._lock:
            buffer = self._buffers.get(thread_id, "") + value
            lines = buffer.split("\n")
            self._buffers[thread_id] = lines.pop()
            for line in lines:
                self._route(line.rstrip())
        return len(value)

    def flush(self) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            line = self._buffers.get(thread_id, "")
            self._buffers[thread_id] = ""
        if line.strip():
            self._route(line.rstrip())

    @staticmethod
    def _route(line: str) -> None:
        if not line:
            return

        account = getattr(_thread_context, "account", None)
        if account is None:
            try:
                _ORIGINAL_STDOUT.write(line + "\n")
            except (OSError, ValueError):
                pass
            return

        level = "info"
        if any(token in line for token in ("❌", "خطأ", "فشل")):
            level = "error"
        elif "⚠️" in line or "⏭️" in line:
            level = "warning"
        elif any(token in line for token in ("✅", "✨", "تم ")):
            level = "success"
        _log(account, line, level)


# ---------------------------------------------------------------------------
# تحميل المحركات وبنوك البيانات


def _load_spx():
    global _spx_module
    with _import_lock:
        if _spx_module is None:
            import spx_solver

            _spx_module = spx_solver
        return _spx_module


def _load_qureo():
    global _qureo_module
    with _import_lock:
        if _qureo_module is None:
            import qureo_solver

            _qureo_module = qureo_solver
        return _qureo_module


def _get_spx_bank():
    global _spx_bank
    with _spx_bank_lock:
        if _spx_bank is None:
            _spx_bank = _load_spx().BankStore()
        return _spx_bank


def _load_qureo_bank():
    global _qureo_answers
    with _qureo_answers_lock:
        if _qureo_answers is None:
            try:
                with open(QUREO_BANK_PATH, "r", encoding="utf-8") as bank_file:
                    loaded = json.load(bank_file)
                _qureo_answers = loaded if isinstance(loaded, dict) else {}
            except FileNotFoundError:
                _qureo_answers = {}
            except (OSError, json.JSONDecodeError) as exc:
                _qureo_answers = {}
                _ORIGINAL_STDOUT.write(
                    f"[warning] تعذرت قراءة بنك Qureo: {exc}\n"
                )
        return _qureo_answers


def _qureo_progress(course, done, total, label):
    account = getattr(_thread_context, "account", None)
    if account is None:
        return
    title = " ".join(part for part in (course, label) if part).strip()
    _set_progress(account, done, total, title)


def _make_fast_login(module, native_login):
    """تسريع تسجيل الدخول مع الإبقاء على مسار الدخول الأصلي كخيار احتياطي."""

    def fast_login(solver, student_id=None, password=None):
        student_code = student_id or getattr(module, "STUDENT_ID", "") or ""
        secret = password or getattr(module, "PASSWORD", "") or ""
        if not (student_code and secret):
            return native_login(solver, student_code, secret)

        if getattr(solver, "turbo", False):
            login_error = getattr(module, "LoginError", RuntimeError)
            try:
                response = solver.context.request.post(
                    module.TP_LOGIN,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(
                        {"student_id": student_code, "password": secret}
                    ),
                )
            except Exception:
                response = None

            if response is not None and response.status in (200, 201, 204, 302):
                try:
                    cookies = (solver.context.storage_state() or {}).get("cookies") or []
                except Exception:
                    cookies = []
                if any(cookie.get("name") == "egy_student_id_token" for cookie in cookies):
                    return None
                raise RuntimeError(
                    "تعذّر تأكيد جلسة المنصة السريعة — جرّب الوضع العادي."
                )

            if response is not None and response.status in (400, 401, 422):
                raise login_error(
                    "فشل تسجيل الدخول — تحقق من الكود وكلمة المرور."
                )
            raise RuntimeError(
                "فشل تسجيل الدخول السريع — تحقق من الكود وكلمة المرور "
                "(أو جرّب الوضع العادي)."
            )

        try:
            response = solver.context.request.post(
                PORTAL_LOGIN,
                headers={"Content-Type": "application/json"},
                data=json.dumps(
                    {"student_id": student_code, "password": secret}
                ),
            )
            if response.status in (200, 201, 204):
                user_info = solver.context.request.get(PORTAL_USERINFO)
                if user_info.status in (200, 201, 204):
                    return None
        except Exception:
            pass

        return native_login(solver, student_code, secret)

    return fast_login


def _install_qureo():
    """تهيئة محرك Qureo مرة واحدة مع بنك مشترك وحفظ آمن للإجابات."""
    global _qureo_installed
    with _import_lock:
        if _qureo_installed:
            return _qureo_module

        module = _load_qureo()
        module.ANSWER_FILE = QUREO_BANK_PATH

        def no_op(_solver):
            return None

        def safe_save_key(_solver, chapter_id, answers):
            if not answers:
                return

            with _qureo_answers_lock:
                bank = _load_qureo_bank()

                # دمج أي بيانات أُضيفت من عملية أخرى قبل الكتابة.
                try:
                    with open(QUREO_BANK_PATH, "r", encoding="utf-8") as bank_file:
                        disk_bank = json.load(bank_file)
                except FileNotFoundError:
                    disk_bank = {}
                except (OSError, json.JSONDecodeError) as exc:
                    _ORIGINAL_STDOUT.write(
                        f"[warning] تعذر دمج بنك Qureo من القرص: {exc}\n"
                    )
                    disk_bank = {}

                if isinstance(disk_bank, dict):
                    for disk_chapter, disk_questions in disk_bank.items():
                        if not isinstance(disk_questions, dict):
                            continue
                        current = bank.setdefault(disk_chapter, {})
                        if isinstance(current, dict):
                            for question_id, answer in disk_questions.items():
                                current.setdefault(question_id, answer)

                chapter_key = str(chapter_id)
                current_answers = bank.setdefault(chapter_key, {})
                if not isinstance(current_answers, dict):
                    current_answers = {}
                    bank[chapter_key] = current_answers

                current_answers.update(
                    {
                        str(question_id): [kind, answer]
                        for question_id, (kind, answer) in answers.items()
                    }
                )

                temporary_path = QUREO_BANK_PATH + ".tmp"
                try:
                    with open(temporary_path, "w", encoding="utf-8") as bank_file:
                        json.dump(bank, bank_file, ensure_ascii=False, indent=2)
                    os.replace(temporary_path, QUREO_BANK_PATH)
                except OSError as exc:
                    try:
                        if os.path.exists(temporary_path):
                            os.remove(temporary_path)
                    except OSError:
                        pass
                    _log(
                        getattr(_thread_context, "account", None) or {"lines": []},
                        f"تعذر حفظ بنك Qureo: {exc}",
                        "warning",
                    )

        solver_class = module.QureoSolver
        if hasattr(solver_class, "_save_key"):
            solver_class._save_key = safe_save_key
        if hasattr(solver_class, "_store_key"):
            solver_class._store_key = safe_save_key
        solver_class._save_answers = no_op
        solver_class.login = _make_fast_login(module, solver_class.login)

        module.PROGRESS = _qureo_progress
        module.SHOULD_STOP = _is_stopped
        _qureo_installed = True
        return module


# ---------------------------------------------------------------------------
# تشغيل محركات الحل


def _run_spx(account, code, password, subject, turbo=False):
    module = _load_spx()
    solver = module.Solver(
        log=lambda message, level="info": _log(account, message, level),
        stop=lambda: _is_stopped(account),
        shared=_get_spx_bank(),
        subject_id=subject,
        turbo=turbo,
    )
    solver.on_progress = lambda done, total, title: _set_progress(
        account, done, total, title
    )
    return solver.run(code, password, subject=subject)


def _run_qureo(account, code, password, courses, turbo=False, lang=None):
    module = _install_qureo()
    answer_bank = _load_qureo_bank()
    _thread_context.account = account
    try:
        solver = module.QureoSolver(
            headless=True,
            courses=courses,
            turbo=turbo,
            lang=lang,
        )
        solver.answers = answer_bank
        solver.run(code, password, courses, close_pause=0)
        return True, "اكتمل حل المسارات: " + "، ".join(courses) + "."
    finally:
        _thread_context.account = None


def _finalize(account, success, message, phase):
    with _state_lock:
        account["running"] = False
        account["finished"] = True
        account["success"] = bool(success)
        account["message"] = str(message)
        account["phase"] = str(phase)


def _worker(account, code, password, mode, options):
    _set_phase(account, "في قائمة الانتظار...")
    if not _acquire_worker_slot(account):
        _finalize(account, False, "أُوقف قبل البدء.", "متوقّف")
        return

    try:
        stop_event = account.get("_stop_event")
        if _is_stopped(account):
            _finalize(account, False, "أُوقف قبل البدء.", "متوقّف")
            return

        _set_phase(account, "جاري التشغيل...")
        try:
            turbo = bool(options.get("turbo", True))
            if mode == "spx":
                success, message = _run_spx(
                    account,
                    code,
                    password,
                    options.get("subject") or "7",
                    turbo,
                )
            elif mode == "qureo2":
                language = str(options.get("lang") or "ar").lower()
                success, message = _run_qureo(
                    account,
                    code,
                    password,
                    ["JavaScript"],
                    turbo,
                    lang=language,
                )
                if success:
                    language_name = "عربي" if language == "ar" else "لغات"
                    message = f"اكتمل حل JavaScript ({language_name}) — الخيار الثاني."
            else:
                success, message = _run_qureo(
                    account,
                    code,
                    password,
                    options.get("courses") or ["Python", "JavaScript"],
                    turbo,
                )
        except Exception as exc:
            error_message = str(exc)
            if "إيقاف" in error_message:
                success, message = False, "تم الإيقاف — حُفظ ما اكتمل."
            else:
                success = False
                message = "خطأ: " + (error_message[:180] or type(exc).__name__)
                _log(account, message, "error")

        final_phase = "انتهى" if success else "توقّف/فشل"
        if stop_event and stop_event.is_set() and not success:
            final_phase = "متوقّف"
        _finalize(account, success, message, final_phase)

    except BaseException as exc:
        error_message = str(exc) or type(exc).__name__
        try:
            _log(account, "تعطّلت العملية فجأة: " + error_message[:200], "error")
        except Exception:
            pass
        _finalize(
            account,
            False,
            "خطأ غير متوقع: " + error_message[:150],
            "فشل",
        )
        raise
    finally:
        _release_worker_slot()


def _running_count() -> int:
    with _state_lock:
        return sum(1 for account in _accounts if account["running"])


def _prune_dead_accounts() -> int:
    """تسوية الحالات التي انتهى خيطها بينما ما زالت مسجلة كعملية جارية."""
    pruned = 0
    with _state_lock:
        for account in _accounts:
            worker = account.get("_thread")
            if account["running"] and (worker is None or not worker.is_alive()):
                account["running"] = False
                account["finished"] = True
                account["success"] = False
                account["message"] = "تعطّلت العملية فجأة — أعد المحاولة."
                account["phase"] = "فشل"
                pruned += 1
    return pruned


def _normalize_accounts(items) -> list[tuple[str, str]]:
    normalized = []
    seen_codes = set()
    if not isinstance(items, list):
        return normalized

    for item in items:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code", "")).strip()
        password = str(item.get("password", "")).strip()
        if code and password and code not in seen_codes:
            seen_codes.add(code)
            normalized.append((code, password))
    return normalized


def _detail_for(job: dict) -> str:
    mode = job["mode"]
    if mode == "spx":
        if str(job.get("subject")) == "7":
            return "الثقافة المالية (عربي)"
        return "Financial Literacy (لغات)"
    if mode == "qureo2":
        language = str(job.get("lang") or "ar").lower()
        language_label = "عربي" if language == "ar" else "لغات"
        return f"Qureo JavaScript ({language_label}) — الخيار الثاني"
    return " + ".join(job.get("courses") or [])


def _start(plan: list[dict], accounts: list[tuple[str, str]]) -> tuple[bool, str]:
    global _accounts
    _prune_dead_accounts()

    with _state_lock:
        if all(not account["running"] for account in _accounts):
            _accounts = []

    new_total = len(accounts) * len(plan)
    running_total = _running_count()
    if running_total + new_total > MAX_ACCOUNTS:
        message = (
            f"الحد الأقصى للعمليات في الانتظار والتشغيل هو {MAX_ACCOUNTS} "
            f"(شغال حالياً: {running_total})."
        )
        return False, message

    _stop_all.clear()
    added = 0
    for code, password in accounts:
        for job in plan:
            with _state_lock:
                already_active = any(
                    account["code"] == code
                    and account["mode"] == job["mode"]
                    and account["running"]
                    for account in _accounts
                )
            if already_active:
                continue

            account = _new_account(code, job["mode"], _detail_for(job))
            with _state_lock:
                _accounts.append(account)

            options = {
                "subject": job.get("subject"),
                "courses": job.get("courses"),
                "lang": job.get("lang"),
                "turbo": bool(job.get("turbo", False)),
            }
            worker = threading.Thread(
                target=_worker,
                args=(account, code, password, job["mode"], options),
                name=f"solver-{job['mode']}-{code}",
                daemon=True,
            )
            account["_thread"] = worker
            worker.start()
            added += 1

    if not added:
        return False, "هذا الحساب قيد التشغيل بالفعل حالياً في العمليات الجارية!"

    if running_total:
        return (
            True,
            f"تمت إضافة {added} عملية جديدة بنجاح وجاري حلها بالتوازي مع العمليات السابقة!",
        )
    return True, f"بدأت العملية على {len(accounts)} كود ({added} عملية)."


# ---------------------------------------------------------------------------
# خادم HTTP المحلي


class _SolverHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def make_handler():
    class Handler(BaseHTTPRequestHandler):
        server_version = "SolverHTTP/2"

        def log_message(self, _format, *_args):
            return None

        def _json(self, status_code: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _error(self, status_code: int, message: str, error_type="platform"):
            self._json(
                status_code,
                {"ok": False, "message": message, "error_type": error_type},
            )

        def _serve_file(self, filename: str, content_type: str) -> bool:
            file_path = os.path.join(APP_DIR, filename)
            try:
                with open(file_path, "rb") as source:
                    content = source.read()
            except OSError:
                return False

            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("X-Content-Type-Options", "nosniff")
            if filename == "index.html":
                self.send_header("Cache-Control", "no-cache")
            else:
                self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            try:
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return True

        @staticmethod
        def _status_payload() -> dict:
            with _state_lock:
                account_rows = [
                    {
                        "id": account.get("id") or account["code"],
                        "code": account["code"],
                        "mode": account["mode"],
                        "detail": account["detail"],
                        "running": account["running"],
                        "finished": account["finished"],
                        "success": account["success"],
                        "message": account["message"],
                        "phase": account["phase"],
                        "progress": dict(account["progress"]),
                        "lines": list(account["lines"]),
                    }
                    for account in _accounts
                ]
                running_total = sum(1 for account in _accounts if account["running"])

            with _concurrency_condition:
                concurrency = _current_concurrency
                active_workers = _active_workers

            return {
                "accounts": account_rows,
                "running_count": running_total,
                "total_count": len(account_rows),
                "concurrency": concurrency,
                "active_workers": active_workers,
                "version": VERSION,
            }

        def _read_json(self) -> dict:
            try:
                length = int(self.headers.get("Content-Length", "0") or 0)
            except ValueError as exc:
                raise ValueError("طول الطلب غير صالح.") from exc

            if length < 0:
                raise ValueError("طول الطلب غير صالح.")
            if length > MAX_REQUEST_BYTES:
                raise OverflowError("حجم البيانات أكبر من الحد المسموح.")
            if length == 0:
                return {}

            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("بيانات الطلب ليست JSON صالحاً.") from exc

            if not isinstance(payload, dict):
                raise ValueError("يجب إرسال كائن JSON.")
            return payload

        def do_GET(self):
            path = urlsplit(self.path).path
            try:
                if path in ("/", "/index.html"):
                    if not self._serve_file("index.html", "text/html; charset=utf-8"):
                        self._error(500, "ملف الواجهة index.html غير موجود.", "server")
                    return

                if path == "/api/health":
                    self._json(200, {"ok": True, "version": VERSION})
                    return

                if path == "/api/status":
                    self._json(200, self._status_payload())
                    return

                self._error(404, "المسار المطلوب غير موجود.", "platform")
            except Exception as exc:
                _ORIGINAL_STDOUT.write(f"[server] GET {path}: {exc}\n")
                self._error(500, "حدث خطأ غير متوقع في الخادم.", "server")

        def _handle_start(self, data: dict) -> None:
            mode = str(data.get("mode") or "").strip()
            if mode not in SUPPORTED_MODES:
                self._error(400, "اختر نوع الحل من الخيارات المتاحة في الواجهة.")
                return

            accounts = _normalize_accounts(data.get("accounts"))
            if not accounts:
                self._error(
                    400,
                    "اكتب كود وكلمة مرور لحساب واحد على الأقل.",
                    "warning",
                )
                return
            if len(accounts) > MAX_ACCOUNTS:
                self._error(
                    400,
                    f"الحد الأقصى للحسابات في الطلب الواحد هو {MAX_ACCOUNTS}.",
                    "warning",
                )
                return

            turbo = bool(data.get("turbo", False))
            plan = []
            if mode == "spx":
                subject = str(data.get("subject") or "7")
                if subject not in ("7", "3"):
                    self._error(400, "المادة المحددة غير مدعومة.")
                    return
                plan.append({"mode": "spx", "subject": subject, "turbo": turbo})

            elif mode == "qureo":
                raw_courses = data.get("courses")
                courses = [
                    course
                    for course in (raw_courses if isinstance(raw_courses, list) else [])
                    if course in ("Python", "JavaScript")
                ]
                courses = list(dict.fromkeys(courses))
                if not courses:
                    self._error(
                        400,
                        "اختر مساراً واحداً على الأقل (Python / JavaScript).",
                        "warning",
                    )
                    return
                plan.append({"mode": "qureo", "courses": courses, "turbo": turbo})

            else:  # qureo2
                language = str(data.get("lang") or "ar").lower()
                if language not in ("ar", "en"):
                    self._error(400, "اختر اللغة أولاً (عربي / لغات).", "warning")
                    return
                plan.append(
                    {
                        "mode": "qureo2",
                        "courses": ["JavaScript"],
                        "lang": language,
                        "turbo": turbo,
                    }
                )

            if "concurrency" in data:
                _set_concurrency(data["concurrency"])

            success, message = _start(plan, accounts)
            self._json(
                200 if success else 409,
                {"ok": success, "message": message, "error_type": "warning"},
            )

        def do_POST(self):
            path = urlsplit(self.path).path
            try:
                if path == "/api/start":
                    try:
                        data = self._read_json()
                    except OverflowError as exc:
                        self._error(413, str(exc), "warning")
                        return
                    except ValueError as exc:
                        self._error(400, str(exc), "warning")
                        return
                    self._handle_start(data)
                    return

                if path == "/api/concurrency":
                    try:
                        data = self._read_json()
                        concurrency = _set_concurrency(data.get("concurrency", DEFAULT_CONCURRENCY))
                    except (ValueError, OverflowError) as exc:
                        self._error(400, str(exc), "warning")
                        return
                    self._json(200, {"ok": True, "concurrency": concurrency})
                    return

                if path == "/api/stop":
                    try:
                        data = self._read_json()
                    except (ValueError, OverflowError):
                        data = {}

                    code_to_stop = str(data.get("code") or "").strip()
                    if code_to_stop:
                        found = False
                        with _state_lock:
                            for account in _accounts:
                                if account["code"] == code_to_stop and account["running"]:
                                    account["_stop_event"].set()
                                    account["phase"] = "جاري الإيقاف..."
                                    found = True
                        message = (
                            f"تم طلب إيقاف الحساب: {code_to_stop}"
                            if found
                            else "الحساب غير قيد التشغيل."
                        )
                        self._json(200, {"ok": True, "message": message})
                        return

                    _stop_all.set()
                    with _state_lock:
                        for account in _accounts:
                            if account["running"]:
                                account["_stop_event"].set()
                    with _concurrency_condition:
                        _concurrency_condition.notify_all()
                    self._json(
                        200,
                        {"ok": True, "message": "جارٍ إيقاف جميع العمليات..."},
                    )
                    return

                if path == "/api/clear_finished":
                    with _state_lock:
                        _accounts[:] = [
                            account for account in _accounts if account["running"]
                        ]
                    self._json(
                        200,
                        {"ok": True, "message": "تم مسح العمليات المكتملة."},
                    )
                    return

                self._error(404, "المسار المطلوب غير موجود.", "platform")
            except Exception as exc:
                _ORIGINAL_STDOUT.write(f"[server] POST {path}: {exc}\n")
                self._error(500, "حدث خطأ غير متوقع في الخادم.", "server")

    return Handler


def main() -> None:
    """تشغيل خادم التطبيق في بيئة الاستضافة أو محلياً."""
    global _ORIGINAL_STDOUT

    # الاحتفاظ بمخرج النظام الأصلي قبل توجيه مخرجات المحركات.
    _ORIGINAL_STDOUT = sys.stdout
    http_server = None

    try:
        sys.stdout = _StdoutSink()

        # تستخدم منصات الاستضافة PORT، والتشغيل المحلي يستخدم PORT_BASE.
        raw_port = os.environ.get("PORT", "").strip()
        if raw_port:
            try:
                selected_port = int(raw_port)
            except ValueError:
                _ORIGINAL_STDOUT.write("[FATAL] قيمة PORT غير صالحة.\n")
                raise SystemExit(1)
        else:
            selected_port = PORT_BASE

        if not 1 <= selected_port <= 65535:
            _ORIGINAL_STDOUT.write("[FATAL] رقم PORT يجب أن يكون بين 1 و65535.\n")
            raise SystemExit(1)

        # يجب أن يستمع الخادم على جميع واجهات الشبكة داخل الحاوية.
        http_server = _SolverHTTPServer(
            ("0.0.0.0", selected_port),
            make_handler(),
        )

        _ORIGINAL_STDOUT.write("[APP] Solver Server started successfully.\n")
        _ORIGINAL_STDOUT.write(f"[APP] Listening on 0.0.0.0:{selected_port}\n")
        _ORIGINAL_STDOUT.write("[APP] Health endpoint: /api/health\n")

        http_server.serve_forever(poll_interval=0.5)

    except KeyboardInterrupt:
        _ORIGINAL_STDOUT.write("\n[APP] Server shutdown requested.\n")

    except SystemExit:
        raise

    except OSError as exc:
        _ORIGINAL_STDOUT.write(f"[FATAL] Server startup/network error: {exc}\n")
        raise SystemExit(1)

    except Exception as exc:
        _ORIGINAL_STDOUT.write(f"[FATAL] Unexpected server error: {exc}\n")
        raise SystemExit(1)

    finally:
        if http_server is not None:
            http_server.server_close()
        sys.stdout = _ORIGINAL_STDOUT


if __name__ == "__main__":
    main()
