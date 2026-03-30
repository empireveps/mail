import json
import time
import base64
import traceback
import os
import threading
import uuid
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from email.message import EmailMessage
from datetime import datetime, timezone
from urllib.parse import urlparse

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/gmail.send']
HOST = '127.0.0.1'
PORT = int(os.getenv('MAIL_CARLOTA_PORT', '8766'))

BASE_DIR = Path(__file__).resolve().parent
TOKEN_PATH = BASE_DIR / 'token.json'
CREDENTIALS_PATH = BASE_DIR / 'credentials.json'
QUEUE_PATH = BASE_DIR / 'scheduled_emails.json'
IMAGES_DIR = Path(r'C:\Users\danha\Downloads\mail\images')
DEFAULT_IMAGE_PATHS = [
    IMAGES_DIR / 'carlota.jpg',
    IMAGES_DIR / 'carlota1.jpg',
    IMAGES_DIR / 'carlota2.jpg',
    IMAGES_DIR / 'carlota3.jpg',
    IMAGES_DIR / 'carlota4.jpg',
]
QUEUE_LOCK = threading.Lock()
SCHEDULER_STARTED = False


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()



def authenticate():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json(), encoding='utf-8')
    return build('gmail', 'v1', credentials=creds)



def html_escape(text: str) -> str:
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')



def default_existing_images():
    return [p for p in DEFAULT_IMAGE_PATHS if p.exists()]



def build_html_email(body_text: str, show_links: bool, image_count: int) -> str:
    paragraphs = []
    for line in str(body_text or '').splitlines():
        paragraphs.append(f'<p>{html_escape(line)}</p>' if line.strip() else '<br>')

    links = ''
    if show_links:
        links = (
            '<p>'
            '<a href="https://www.carlotacosmetics.com" target="_blank">Carlota.com</a> | '
            '<a href="https://www.instagram.com/carlotacosmetics" target="_blank">Instagram</a> | '
            '<a href="https://www.tiktok.com/@carlotacosmetics" target="_blank">TikTok</a>'
            '</p>'
        )

    images_html = ''.join(
        f'<img src="cid:image{i}" width="500" style="display:block;margin:0 auto 18px auto;max-width:100%;border-radius:14px;"><br>'
        for i in range(1, image_count + 1)
    )

    return f"""<html>
  <body style="font-family: Arial, sans-serif; line-height: 1.6; color:#111;">
    {''.join(paragraphs)}
    {links}
    <div style="text-align:center; margin-top: 18px;">
      {images_html}
    </div>
  </body>
</html>
"""



def resolve_image_paths(image_paths):
    base_images = default_existing_images()
    resolved = list(base_images)
    for image_path in image_paths or []:
        p = Path(image_path)
        if not p.is_absolute():
            p = BASE_DIR / p
        if p.exists() and p not in resolved:
            resolved.append(p)
    return resolved



def send_email(service, to, subject, html_content, image_paths):
    message = EmailMessage()
    sender_name = "Eden | Carlota Cosmetics"
    sender_email = "eden@carlotacosmetics.com"
    message['From'] = f"{sender_name} <{sender_email}>"
    message['To'] = to
    message['Subject'] = subject
    message.add_alternative(html_content, subtype='html')
    html_part = message.get_payload()[0]
    for i, path in enumerate(image_paths, start=1):
        if path.exists():
            with open(path, 'rb') as img:
                subtype = path.suffix.replace('.', '').lower() or 'jpeg'
                html_part.add_related(img.read(), maintype='image', subtype=subtype, cid=f'image{i}')
    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId="me", body={'raw': encoded_message}).execute()



def ensure_queue_file():
    if not QUEUE_PATH.exists():
        QUEUE_PATH.write_text('[]\n', encoding='utf-8')



def load_queue():
    ensure_queue_file()
    try:
        return json.loads(QUEUE_PATH.read_text(encoding='utf-8'))
    except Exception:
        return []



def save_queue(items):
    QUEUE_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')



def queue_job(subject, emails, body_text, show_links=True, image_paths=None, scheduled_for=None):
    full_images = [str(p) for p in resolve_image_paths(image_paths)]
    job = {
        'id': str(uuid.uuid4()),
        'subject': subject,
        'emails': emails,
        'body_text': body_text,
        'show_links': bool(show_links),
        'image_paths': full_images,
        'scheduled_for': scheduled_for,
        'status': 'scheduled',
        'created_at': utc_now_iso(),
        'updated_at': utc_now_iso(),
        'sent_count': 0,
        'error': None,
    }
    with QUEUE_LOCK:
        items = load_queue()
        items.append(job)
        save_queue(items)
    return job



def list_jobs():
    with QUEUE_LOCK:
        return load_queue()



def update_job(updated_job):
    updated_job['updated_at'] = utc_now_iso()
    with QUEUE_LOCK:
        items = load_queue()
        for i, item in enumerate(items):
            if item.get('id') == updated_job.get('id'):
                items[i] = updated_job
                break
        save_queue(items)



def cancel_job(job_id):
    with QUEUE_LOCK:
        items = load_queue()
        for index, item in enumerate(items):
            if item.get('id') == job_id:
                if item.get('status') != 'scheduled':
                    raise ValueError('Only scheduled jobs can be cancelled')
                removed = items.pop(index)
                save_queue(items)
                return removed
    raise ValueError('Job not found')



def edit_job(job_id, payload):
    with QUEUE_LOCK:
        items = load_queue()
        found = None
        for item in items:
            if item.get('id') == job_id:
                found = item
                break
        if not found:
            raise ValueError('Job not found')
        if found.get('status') != 'scheduled':
            raise ValueError('Only scheduled jobs can be edited')
        found['subject'] = str(payload.get('subject', found.get('subject', ''))).strip() or found.get('subject', '')
        found['emails'] = [str(e).strip() for e in payload.get('emails', found.get('emails', [])) if str(e).strip()]
        found['body_text'] = str(payload.get('body_text', found.get('body_text', '')))
        found['scheduled_for'] = payload.get('scheduled_for', found.get('scheduled_for'))
        found['image_paths'] = [str(p) for p in resolve_image_paths(payload.get('image_paths', found.get('image_paths', [])))]
        found['updated_at'] = utc_now_iso()
        save_queue(items)
        return found



def send_job(job):
    service = authenticate()
    image_paths = resolve_image_paths(job.get('image_paths', []))
    html_content = build_html_email(job.get('body_text', ''), bool(job.get('show_links', True)), len(image_paths))
    sent = 0
    for email in job.get('emails', []):
        send_email(service, email, job.get('subject', ''), html_content, image_paths)
        sent += 1
        time.sleep(1.2)
    job['status'] = 'sent'
    job['sent_count'] = sent
    job['sent_at'] = utc_now_iso()
    job['error'] = None
    update_job(job)
    return sent



def scheduler_loop():
    while True:
        try:
            now = datetime.now(timezone.utc)
            jobs = list_jobs()
            for job in jobs:
                if job.get('status') != 'scheduled':
                    continue
                scheduled_for = job.get('scheduled_for')
                if not scheduled_for:
                    continue
                try:
                    target = datetime.fromisoformat(scheduled_for.replace('Z', '+00:00'))
                except Exception:
                    job['status'] = 'failed'
                    job['error'] = 'Invalid scheduled_for datetime'
                    update_job(job)
                    continue
                if target <= now:
                    try:
                        send_job(job)
                    except Exception as exc:
                        job['status'] = 'failed'
                        job['error'] = str(exc)
                        job['failed_at'] = utc_now_iso()
                        update_job(job)
            time.sleep(5)
        except Exception:
            time.sleep(5)



def start_scheduler_once():
    global SCHEDULER_STARTED
    if SCHEDULER_STARTED:
        return
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    SCHEDULER_STARTED = True


class Handler(BaseHTTPRequestHandler):
    service = None

    def log_message(self, format, *args):
        return

    def _set_headers(self, status=200, content_type='application/json; charset=utf-8'):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS')
        self.end_headers()

    def _send_json(self, status, payload):
        self._set_headers(status)
        self.wfile.write(json.dumps(payload, ensure_ascii=False).encode('utf-8'))

    def _read_json(self):
        content_length = int(self.headers.get('Content-Length', '0'))
        return json.loads(self.rfile.read(content_length).decode('utf-8')) if content_length else {}

    def do_OPTIONS(self):
        self._set_headers(204, 'text/plain; charset=utf-8')

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path in ('/', '/health', '/status'):
                payload = {'ok': True, 'service': 'mail-carlota-backend', 'gmail_ready': False}
                if Handler.service is None:
                    Handler.service = authenticate()
                payload['gmail_ready'] = True
                payload['sender_email'] = 'eden@carlotacosmetics.com'
                payload['port'] = PORT
                payload['scheduled_jobs'] = len([j for j in list_jobs() if j.get('status') == 'scheduled'])
                payload['default_images'] = [str(p) for p in default_existing_images()]
                self._send_json(200, payload)
                return
            if parsed.path == '/queue':
                self._send_json(200, {'ok': True, 'jobs': list_jobs()})
                return
            self._send_json(404, {'ok': False, 'error': 'Not found'})
        except Exception as exc:
            self._send_json(500, {'ok': False, 'error': str(exc), 'type': exc.__class__.__name__, 'trace': traceback.format_exc()})

    def do_POST(self):
        try:
            if self.path not in ('/send', '/schedule', '/queue/cancel'):
                self._send_json(404, {'ok': False, 'error': 'Not found'})
                return
            payload = self._read_json()
            if self.path == '/queue/cancel':
                job_id = str(payload.get('job_id', '')).strip()
                if not job_id:
                    raise ValueError('job_id requis')
                removed = cancel_job(job_id)
                self._send_json(200, {'ok': True, 'deleted': True, 'job': removed})
                return

            subject = str(payload.get('subject', '')).strip()
            emails = [str(e).strip() for e in payload.get('emails', []) if str(e).strip()]
            body_text = str(payload.get('body_text', ''))
            show_links = bool(payload.get('show_links', True))
            image_paths = resolve_image_paths(payload.get('image_paths', []))
            scheduled_for = payload.get('scheduled_for')
            if not subject:
                raise ValueError('Subject vide')
            if not emails:
                raise ValueError('Aucun email fourni')
            if self.path == '/schedule':
                if not scheduled_for:
                    raise ValueError('scheduled_for requis')
                job = queue_job(subject, emails, body_text, show_links=show_links, image_paths=[str(p) for p in image_paths], scheduled_for=scheduled_for)
                self._send_json(200, {'ok': True, 'scheduled': True, 'job_id': job['id'], 'scheduled_for': job['scheduled_for'], 'email_count': len(job['emails'])})
                return
            html_content = build_html_email(body_text, show_links, len(image_paths))
            if Handler.service is None:
                Handler.service = authenticate()
            sent = 0
            for email in emails:
                send_email(Handler.service, email, subject, html_content, image_paths)
                sent += 1
                time.sleep(1.2)
            self._send_json(200, {'ok': True, 'sent_count': sent, 'sender_email': 'eden@carlotacosmetics.com', 'port': PORT, 'images_attached': len(image_paths)})
        except Exception as exc:
            self._send_json(500, {'ok': False, 'error': str(exc), 'type': exc.__class__.__name__, 'trace': traceback.format_exc()})

    def do_PUT(self):
        try:
            if self.path != '/queue/edit':
                self._send_json(404, {'ok': False, 'error': 'Not found'})
                return
            payload = self._read_json()
            job_id = str(payload.get('job_id', '')).strip()
            if not job_id:
                raise ValueError('job_id requis')
            job = edit_job(job_id, payload)
            self._send_json(200, {'ok': True, 'job': job})
        except Exception as exc:
            self._send_json(500, {'ok': False, 'error': str(exc), 'type': exc.__class__.__name__, 'trace': traceback.format_exc()})



def main():
    ensure_queue_file()
    start_scheduler_once()
    server = HTTPServer((HOST, PORT), Handler)
    print(f'Server running on http://{HOST}:{PORT}')
    print(f'Auto images: {len(default_existing_images())} image(s) will be appended to every email.')
    server.serve_forever()


if __name__ == '__main__':
    main()
