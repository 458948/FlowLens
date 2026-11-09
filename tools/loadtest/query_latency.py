#!/usr/bin/env python3
"""Persistent-connection HTTP query latency probe. Stdlib only."""
import argparse
import http.client
import time

def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(q * len(sorted_vals)) - (0 if q < 1 else 1)))
    return sorted_vals[idx]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=8080)
    ap.add_argument('--path', required=True)
    ap.add_argument('--count', type=int, default=10000)
    ap.add_argument('--concurrency', type=int, default=1)
    ap.add_argument('--reconnect', action='store_true')
    args = ap.parse_args()

    lat = []
    statuses = {}
    failures = 0
    conn = http.client.HTTPConnection(args.host, args.port, timeout=15.0)

    for i in range(args.count):
        if args.reconnect:
            try:
                conn.close()
            except Exception:
                pass
            conn = http.client.HTTPConnection(args.host, args.port, timeout=15.0)
        t0 = time.perf_counter()
        try:
            conn.request('GET', args.path, headers={'Connection': 'keep-alive'})
            resp = conn.getresponse()
            body = resp.read()
            statuses[resp.status] = statuses.get(resp.status, 0) + 1
            if 200 <= resp.status < 300:
                lat.append((time.perf_counter() - t0) * 1000.0)
            else:
                failures += 1
            resp.close()
        except Exception:
            failures += 1
            # Reconnect for the next request.
            try:
                conn.close()
            except Exception:
                pass
            conn = http.client.HTTPConnection(args.host, args.port, timeout=15.0)

    conn.close()
    lat.sort()
    print(f'requests={args.count} success={len(lat)} failures={failures} statuses={statuses}')
    print(f'p50={percentile(lat,0.50):.3f} p95={percentile(lat,0.95):.3f} p99={percentile(lat,0.99):.3f} max={lat[-1] if lat else 0.0:.3f}')

if __name__ == '__main__':
    main()