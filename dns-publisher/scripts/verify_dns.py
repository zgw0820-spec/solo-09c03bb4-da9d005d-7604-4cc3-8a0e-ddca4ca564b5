#!/usr/bin/env python3
"""运维核对：直接向两台权威节点发 A 查询，比对实际答复是否一致。

UDP 查询携带 EDNS(0)（通告 4096 字节接收上限）。若答复置 TC（整组地址
超出 UDP 载荷上限，节点按规定不返回部分地址），自动改用 TCP 重新查询，
以 TCP 返回的完整地址组参与比对——截断绝不等于"少地址"。

用法:
  python verify_dns.py device.example.internal 127.0.0.1:10053 127.0.0.1:10054

任一节点不可达、TC 后 TCP 核对失败、答复不一致或非权威(AA=0)时退出码为 1。
"""
from __future__ import annotations

import socket
import struct
import sys

from dnslib import EDNS0, QTYPE, RCODE, DNSRecord

EDNS_UDP_SIZE = 4096


def _udp_question(domain: str) -> bytes:
    q = DNSRecord.question(domain, qtype="A")
    q.add_ar(EDNS0(udp_len=EDNS_UDP_SIZE, version=0))
    return q.pack()


def _tcp_query(host: str, port: int, packet: bytes,
               timeout: float = 3.0) -> DNSRecord:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.sendall(struct.pack("!H", len(packet)) + packet)
        header = b""
        while len(header) < 2:
            part = s.recv(2 - len(header))
            if not part:
                raise ConnectionError("TCP 连接在长度前缀前关闭")
            header += part
        (msg_len,) = struct.unpack("!H", header)
        chunks, received = [], 0
        while received < msg_len:
            part = s.recv(min(65535, msg_len - received))
            if not part:
                raise ConnectionError("TCP 应答中途关闭")
            chunks.append(part)
            received += len(part)
        return DNSRecord.parse(b"".join(chunks))
    finally:
        s.close()


def query(host: str, port: int, domain: str, timeout: float = 3.0) -> set[str]:
    packet = _udp_question(domain)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(packet, (host, port))
        data, _ = s.recvfrom(65535)
    finally:
        s.close()
    udp_reply = DNSRecord.parse(data)

    if udp_reply.header.tc:
        # TC=1: 节点有意不返回部分地址，必须走 TCP 取整组。
        reply = _tcp_query(host, port, packet, timeout)
        transport = "TCP（UDP 答复 TC，已回退）"
    else:
        reply = udp_reply
        transport = "UDP"

    if not reply.header.aa:
        raise RuntimeError(f"{host}:{port} 应答 AA=0，非权威应答")
    rcode = RCODE[reply.header.rcode]
    if rcode != "NOERROR":
        raise RuntimeError(f"{host}:{port} RCODE={rcode}")
    answers = {str(rr.rdata) for rr in reply.rr if QTYPE[rr.rtype] == "A"}
    if not answers:
        raise RuntimeError(f"{host}:{port} {transport} 应答为空")
    print(f"  ({transport})")
    return answers


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
            print(f"\n两端实际 DNS 答复一致（{len(next(iter(unique)))} 条地址）: "
                  f"{sorted(next(iter(unique)))}")
            return 0
        failed = True
        print("\n两端答复不一致，不得切流!")
    print("\n核对未通过，禁止安排切流。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
