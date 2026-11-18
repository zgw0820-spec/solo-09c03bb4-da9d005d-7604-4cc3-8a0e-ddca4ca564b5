#!/usr/bin/env python3
"""运维核对：直接向两台权威节点发 UDP A 查询，比对实际答复是否一致。

用法:
  python verify_dns.py device.example.internal 127.0.0.1:10053 127.0.0.1:10054

任一节点不可达、答复不一致或非权威(AA=0)时退出码为 1。
"""
from __future__ import annotations

import socket
import sys

from dnslib import QTYPE, RCODE, DNSRecord


def query(host: str, port: int, domain: str, timeout: float = 3.0) -> set[str]:
    pkt = DNSRecord.question(domain, qtype="A")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(pkt.pack(), (host, port))
        data, _ = s.recvfrom(65535)
    finally:
        s.close()
    reply = DNSRecord.parse(data)
    if not reply.header.aa:
        raise RuntimeError(f"{host}:{port} 应答 AA=0，非权威应答")
    rcode = RCODE[reply.header.rcode]
    if rcode != "NOERROR":
        raise RuntimeError(f"{host}:{port} RCODE={rcode}")
    return {str(rr.rdata) for rr in reply.rr if QTYPE[rr.rtype] == "A"}


def main() -> int:
    if len(sys.argv) < 4:
        print(__doc__)
        return 2
    domain = sys.argv[1]
    targets = [t.rsplit(":", 1) for t in sys.argv[2:]]

    answers: dict[str, set[str]] = {}
    failed = False
    for host, port_s in targets:
        target = f"{host}:{port_s}"
        try:
            answers[target] = query(host, int(port_s), domain)
            print(f"[OK]   {target:24s} -> {sorted(answers[target])}")
        except Exception as exc:
            failed = True
            print(f"[FAIL] {target:24s} -> {exc}")

    if not failed:
        unique = {frozenset(v) for v in answers.values()}
        if len(unique) == 1:
            print(f"\n两端实际 DNS 答复一致: {sorted(next(iter(unique)))}")
            return 0
        failed = True
        print("\n两端答复不一致，不得切流!")
    print("\n核对未通过，禁止安排切流。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
