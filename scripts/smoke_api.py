import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


def main():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    base = f'http://127.0.0.1:{port}'
    env = dict(os.environ)
    env['PYTHONPATH'] = os.pathsep.join(filter(None, ['src', env.get('PYTHONPATH')]))
    with tempfile.TemporaryFile(mode='w+') as log:
        server = subprocess.Popen(
            [sys.executable, '-m', 'uvicorn', 'dre.api.main:app', '--host', '127.0.0.1', '--port', str(port)],
            env=env, stdout=log, stderr=log,
        )
        try:
            for _ in range(100):
                if server.poll() is not None:
                    raise RuntimeError('Server exited before startup')
                try:
                    with urlopen(base + '/openapi.json', timeout=1):
                        break
                except (URLError, TimeoutError):
                    time.sleep(0.1)
            else:
                raise RuntimeError('Server did not become ready')

            def request(path, params=None, expected=200):
                url = base + path + ('?' + urlencode(params) if params else '')
                try:
                    response = urlopen(url, timeout=20)
                except HTTPError as error:
                    response = error
                with response:
                    assert response.status == expected, (url, response.status, response.read())
                    return json.load(response)

            assert request('/health')['transactions'] > 0
            assert request('/areas')['areas']
            text = request('/estimate', {'q': 'Dubai Marina, 2-bedroom, 1,200 sq ft'})
            structured = request('/estimate/structured', {'area': 'Dubai Marina', 'bedrooms': 2, 'size_sqft': 1200})
            assert text == structured
            assert text['sales']['median_price_per_sqft_aed'] > 0
            assert len(text['comparables']) == 10
            assert 'Not a valuation' in text['disclaimer']
            request('/estimate/structured', {'area': 'Unknown test area'}, expected=404)
            request('/estimate/structured', {'area': 'Dubai Marina', 'size_sqft': -1}, expected=422)
            request('/estimate/structured', {'area': 'Dubai Marina', 'bedrooms': 11}, expected=422)
            request('/estimate', expected=422)
            print('PASS: live HTTP health, areas, both estimates, parity, 404 and validation errors')
        except Exception:
            log.seek(0)
            print(log.read(), file=sys.stderr)
            raise
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


if __name__ == '__main__':
    main()
