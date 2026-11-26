"""نقطة فحص بسيطة لبيئة Vercel القديمة.

التشغيل الفعلي متعدد الحسابات يعتمد على الخادم المحلي المستمر في server.py؛
هذه الدالة لا تشغّل محركات الحل ولا تستبدل الخادم المحلي.
"""

import json
from http.server import BaseHTTPRequestHandler


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload = {
            "status": "success",
            "message": "The local solver service is available through server.py.",
        }
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        payload = {
            "ok": False,
            "error_type": "platform",
            "message": "يتطلب تشغيل العمليات الخادم المحلي المستمر server.py.",
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(501)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
