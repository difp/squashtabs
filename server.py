#!/usr/bin/env python3
"""Локальный сервер для «Сквош · сетка и судейство».

Раздаёт сам файл приложения и работает как ntfy-совместимый обмен для режима
«Поделиться». Публичные сервисы не нужны: и приложение, и обмен идут по вашей сети.

Запуск:   python3 server.py                     # порт 8080, без ключа
          python3 server.py 9000                # свой порт
          python3 server.py 9000 --key s7k2p9   # обмен только для тех, кто знает ключ

Ключ нужен, если сервер открывается в интернет через туннель (см. README). Тогда
адрес сервера в приложении указывают вместе с ключом:  https://туннель/s7k2p9

Адреса сервер печатает при запуске. Требуется только Python 3, интернет не нужен.
"""
import itertools
import json
import os
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

ROOT = os.path.dirname(os.path.abspath(__file__))
STORE = {}          # topic -> [сообщение, ...]
SUBS = {}           # topic -> [очередь подписчика, ...]
LOCK = threading.Lock()
IDS = itertools.count(1)
KEY = ''            # если задан — обмен доступен только по адресу /<KEY>/...

MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.json': 'application/json; charset=utf-8',
    '.md': 'text/markdown; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.ico': 'image/x-icon',
}


def since_filter(msgs, since):
    """Оставить сообщения после указанного id (или все, если since=all)."""
    if not since or since == 'all':
        return msgs
    idx = next((i for i, m in enumerate(msgs) if m['id'] == since), -1)
    return msgs[idx + 1:] if idx >= 0 else msgs


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    server_version = 'SquashGrid'

    def log_message(self, *args):
        pass                                  # не засоряем консоль

    # ---------- служебное ----------
    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,PUT,POST,PATCH,DELETE,OPTIONS')

    def _send(self, body, ctype):
        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self._cors()
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _parts(self):
        u = urlparse(self.path)
        return [unquote(p) for p in u.path.strip('/').split('/') if p], u

    def _resolve(self, parts):
        """Разобрать путь обмена с учётом ключа: /[<KEY>/]<комната>[/sse|/json]."""
        if KEY:
            if not parts or parts[0] != KEY:
                return None                # без ключа обмен закрыт
            parts = parts[1:]
        if not parts:
            return None
        return parts[0], (parts[1] if len(parts) > 1 else '')

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_HEAD(self):
        self.send_response(200)
        self._cors()
        self.send_header('Content-Length', '0')
        self.end_headers()

    # ---------- публикация: POST/PUT /[<KEY>/]<комната> ----------
    def _publish(self):
        parts, _ = self._parts()
        r = self._resolve(parts)
        if not r:
            return self.send_error(403, 'key required' if KEY else 'no topic')
        topic = r[0]
        n = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(n).decode('utf-8') if n else ''
        msg = {'id': 'm%05d' % next(IDS), 'time': int(time.time()),
               'event': 'message', 'topic': topic, 'message': body}
        with LOCK:
            STORE.setdefault(topic, []).append(msg)
            for q in SUBS.get(topic, []):
                q.append(msg)
        self._send(json.dumps(msg, ensure_ascii=False).encode(), 'application/json; charset=utf-8')

    do_POST = _publish
    do_PUT = _publish

    # ---------- чтение ----------
    def do_GET(self):
        parts, u = self._parts()
        r = self._resolve(parts)
        if r:
            topic, action = r
            if action == 'sse':
                return self._sse(topic, u)
            if action == 'json':
                return self._poll(topic, u)
        # всё остальное — файлы приложения (при ключе путь может быть /<KEY>/ или /<KEY>/index.html)
        raw = parts[1:] if (KEY and parts and parts[0] == KEY) else parts
        if not raw or raw[0] in ('index.html',):
            return self._static('index.html')
        if len(raw) == 1:
            return self._static(raw[0])
        return self.send_error(404)

    def _static(self, name):
        path = os.path.join(ROOT, name)
        if not os.path.isfile(path):
            return self.send_error(404)
        with open(path, 'rb') as f:
            data = f.read()
        ext = os.path.splitext(path)[1].lower()
        self.send_response(200)
        self.send_header('Content-Type', MIME.get(ext, 'application/octet-stream'))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _sse(self, topic, u):
        """Поток сообщений комнаты — то, что приложение слушает постоянно."""
        since = (parse_qs(u.query).get('since') or ['all'])[0]
        with LOCK:
            msgs = since_filter(list(STORE.get(topic, [])), since)
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        self._cors()
        self.end_headers()
        queue = []
        try:
            for m in msgs:
                self.wfile.write(('data: ' + json.dumps(m, ensure_ascii=False) + '\n\n').encode())
                self.wfile.flush()
            with LOCK:
                SUBS.setdefault(topic, []).append(queue)
            while True:
                if queue:
                    m = queue.pop(0)
                    self.wfile.write(('data: ' + json.dumps(m, ensure_ascii=False) + '\n\n').encode())
                    self.wfile.flush()
                else:
                    time.sleep(0.15)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with LOCK:
                if topic in SUBS and queue in SUBS[topic]:
                    SUBS[topic].remove(queue)

    def _poll(self, topic, u):
        """Разовый опрос — резервный путь, если поток недоступен."""
        since = (parse_qs(u.query).get('since') or ['all'])[0]
        with LOCK:
            msgs = since_filter(list(STORE.get(topic, [])), since)
        out = ''.join(json.dumps(m, ensure_ascii=False) + '\n' for m in msgs).encode()
        self._send(out, 'application/x-ndjson; charset=utf-8')


def local_ips():
    ips = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ips.append(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith('127.') and ip not in ips:
                ips.append(ip)
    except OSError:
        pass
    return ips


def main():
    global KEY
    args = [a for a in sys.argv[1:]]
    port = 8080
    if args and args[0].isdigit():
        port = int(args.pop(0))
    if '--key' in args:
        i = args.index('--key')
        if i + 1 < len(args):
            KEY = args[i + 1].strip('/ ')
        args = []
    try:
        httpd = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    except OSError:
        print(f'Порт {port} занят. Запустите на другом порту:\n'
              f'    python3 server.py {port + 1}', flush=True)
        return
    ips = local_ips() or ['<IP-устройства>']
    suffix = f'/{KEY}' if KEY else ''
    out = ['Сквош · сетка и судейство — локальный сервер', f'Порт: {port}', '']
    for ip in ips:
        out.append(f'  приложение:  http://{ip}:{port}/')
        out.append(f'  адрес сервера для «Поделиться»:  http://{ip}:{port}{suffix}')
    if KEY:
        out += ['', f'Ключ доступа: {KEY} — без него обмен не работает.',
                'Если открываете сервер через туннель, адрес будет вида',
                f'    https://<туннель>{suffix}']
    out += ['', 'Ctrl+C — остановить']
    print('\n'.join(out), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nОстановлено', flush=True)


if __name__ == '__main__':
    main()
