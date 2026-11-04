"""Single-node ROS 2 benchmark probe (throughput and latency).

Usage:
    python3 -u bench_probe.py --mode pub|sub --topic T
        [--count N] [--rate HZ] [--size BYTES]
        [--qos-reliability reliable|best_effort]
        [--qos-durability volatile|transient_local]
        [--qos-history keep_last|keep_all] [--qos-depth D]
        [--timeout S] [--run-id ID] [--wait-for-match S]

Modes: pub publishes N payloads on T at rate; sub subscribes on T
until N unique seqs received or timeout.

Wait for match: pub with --wait-for-match S waits up to S seconds
for a matched reader before the first publish; timeout exits 2
since a readerless bench publish is invalid data. Default 0 is off.

Modes: pub publishes N payloads on T at rate; sub subscribes on T
until N unique seqs received or timeout.

Payload: std_msgs/msg/String.data holds exactly size characters:
chars 0..31 are the ASCII hex of struct.pack('<QQ', send_ns, seq)
(send_ns is time.time_ns at publish, seq starts at 1), and the rest
is deterministic NUL-free fill chr(33 + ((seq+i) % 94)).

Wire deviation: the contract's binary header plus bytes((seq+i)%256)
fill cannot cross rclpy. String truncates at the first NUL byte
(verified in-container: a 256-char latin-1 payload arrives empty and
'AB\x00CD' arrives as 'AB'), so the NUL high bytes that seq always
contributes destroy the header and tail for every subscriber, in any
language - the truncation happens in the send conversion. The header
therefore travels hex-encoded (same 16 bytes, first 16 hex chars
still decode to send_ns) and the fill uses printable ASCII; .data
stays exactly size characters, all ASCII, so the wire size matches
too. Minimum --size is 33 (32-char header plus fill).

QoS: a QoSProfile built from the four qos flags only, nothing else.

Output: exactly one JSON document on stdout; all logging on stderr.

Exit codes: 0 on completion, even with gaps or partial receipt
(gaps are data); 2 on bad args, or timeout with zero received, which
marks discovery failure; 1 on crash; 130 on interrupt.
"""

import argparse
import json
import struct
import sys
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy
from rclpy.qos import HistoryPolicy
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy
from std_msgs.msg import String

HEADER_FMT = "<QQ"
HEADER_CHARS = 32
MIN_SIZE = HEADER_CHARS + 1
SPIN_STEP = 0.1
TIMEOUT_PAD = 30.0


def log(text):
    """Write one status line to stderr (stdout holds only JSON)."""
    print("bench_probe: %s" % text, file=sys.stderr)


def sanitize(value):
    """Fold text into a valid ROS node name fragment (a-z, 0-9, _)."""
    chars = []
    for char in value.lower():
        if "a" <= char <= "z" or "0" <= char <= "9" or char == "_":
            chars.append(char)
        else:
            chars.append("_")
    text = "".join(chars).strip("_")
    if not text or not text[0].isalpha():
        text = "n" + text
    return text


def parse_args(argv):
    """Parse flags; bad values exit 2 via parser.error."""
    parser = argparse.ArgumentParser(
        description="Single-node ROS 2 benchmark probe.",
        epilog="Exit 0 on completion (gaps are data). Exit 2 on bad "
        "args or timeout with zero received (discovery failure). "
        "Timeout defaults to count/rate + 30s.",
    )
    parser.add_argument("--mode", required=True,
                        choices=("pub", "sub"))
    parser.add_argument("--topic", required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seq-base", type=int, default=1)
    parser.add_argument("--rate", type=float, default=10.0)
    parser.add_argument("--size", type=int, default=256)
    parser.add_argument("--qos-reliability", default="reliable")
    parser.add_argument("--qos-durability", default="volatile")
    parser.add_argument("--qos-history", default="keep_last")
    parser.add_argument("--qos-depth", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--wait-for-match", type=float, default=0.0)
    args = parser.parse_args(argv)
    args.qos_reliability = args.qos_reliability.lower().replace("-", "_")
    args.qos_durability = args.qos_durability.lower().replace("-", "_")
    args.qos_history = args.qos_history.lower().replace("-", "_")
    if args.qos_reliability not in ("reliable", "best_effort"):
        parser.error("qos-reliability must be reliable or best_effort")
    if args.qos_durability not in ("volatile", "transient_local"):
        parser.error("qos-durability must be volatile or transient_local")
    if args.qos_history not in ("keep_last", "keep_all"):
        parser.error("qos-history must be keep_last or keep_all")
    if args.count < 1:
        parser.error("--count must be >= 1")
    if args.seq_base < 1:
        parser.error("--seq-base must be >= 1")
    if args.rate <= 0:
        parser.error("--rate must be > 0")
    if args.size < MIN_SIZE:
        parser.error("--size must be >= %d (hex header plus fill)" % MIN_SIZE)
    if args.qos_depth < 1:
        parser.error("--qos-depth must be >= 1")
    if args.timeout is not None and args.timeout <= 0:
        parser.error("--timeout must be > 0")
    if args.wait_for_match < 0:
        parser.error("--wait-for-match must be >= 0")
    if not args.run_id:
        args.run_id = "run-%d" % time.time_ns()
    if args.timeout is None:
        args.timeout = args.count / args.rate + TIMEOUT_PAD
    return args


def build_qos(args):
    """Build a QoSProfile from the four qos flags; return profile+names."""
    reliability = {
        "reliable": ReliabilityPolicy.RELIABLE,
        "best_effort": ReliabilityPolicy.BEST_EFFORT,
    }[args.qos_reliability]
    durability = {
        "volatile": DurabilityPolicy.VOLATILE,
        "transient_local": DurabilityPolicy.TRANSIENT_LOCAL,
    }[args.qos_durability]
    history = {
        "keep_last": HistoryPolicy.KEEP_LAST,
        "keep_all": HistoryPolicy.KEEP_ALL,
    }[args.qos_history]
    profile = QoSProfile(
        reliability=reliability,
        durability=durability,
        history=history,
        depth=args.qos_depth,
    )
    names = {
        "reliability": args.qos_reliability,
        "durability": args.qos_durability,
        "history": args.qos_history,
        "depth": args.qos_depth,
    }
    return profile, names


def make_data(seq, size, send_ns):
    """Build the size-char payload; header is hex of the spec buffer.

    rclpy truncates String at the first NUL byte, so the binary
    struct.pack buffer cannot cross the wire; the header travels as
    32 ASCII hex chars and the fill uses NUL-free printable ASCII.
    """
    raw = struct.pack(HEADER_FMT, send_ns, seq)
    fill = bytes(33 + ((seq + i) % 94) for i in range(size - HEADER_CHARS))
    return raw.hex() + fill.decode("ascii")


def split_data(text):
    """Recover (send_ns, seq) from a received String payload."""
    if len(text) < HEADER_CHARS:
        raise ValueError("payload shorter than header")
    send_ns = int.from_bytes(bytes.fromhex(text[:16]), "little")
    seq = int.from_bytes(bytes.fromhex(text[16:HEADER_CHARS]), "little")
    return send_ns, seq


def make_node(mode, run_id):
    """Create a uniquely named node holding mode and run id."""
    base = sanitize("bench_%s_%s" % (mode, run_id))
    return Node("%s_%d" % (base, time.time_ns()))


def blank_result(args, qos_names):
    """Build the completion JSON skeleton with null timestamps."""
    return {
        "mode": args.mode,
        "topic": args.topic,
        "qos": qos_names,
        "count": args.count,
        "sent": 0,
        "received": 0,
        "gaps": [],
        "dups": 0,
        "send_first_ns": None,
        "send_last_ns": None,
        "recv_first_ns": None,
        "recv_last_ns": None,
        "lat_ns": [],
        "note": "",
    }


class SeqTrack:
    """Seen seqs with dup count."""

    def __init__(self):
        self.seen = set()
        self.dups = 0

    def add(self, seq):
        """Record seq; True when first seen, count a dup otherwise."""
        if seq in self.seen:
            self.dups += 1
            return False
        self.seen.add(seq)
        return True

    def gaps(self, count):
        """Sorted seqs in 1..count never seen."""
        return [seq for seq in range(1, count + 1) if seq not in self.seen]


def mark_sent(res, send_ns):
    """Fold one publish stamp into the result."""
    if res["send_first_ns"] is None:
        res["send_first_ns"] = send_ns
    res["send_last_ns"] = send_ns
    res["sent"] += 1


def mark_recv(res, recv_ns):
    """Fold one reception stamp into the result."""
    if res["recv_first_ns"] is None:
        res["recv_first_ns"] = recv_ns
    res["recv_last_ns"] = recv_ns
    res["received"] += 1


def publish_seq(pub, seq, size):
    """Publish one payload; return its send_ns stamp."""
    send_ns = time.time_ns()
    msg = String()
    msg.data = make_data(seq, size, send_ns)
    pub.publish(msg)
    return send_ns


def spin_until(node, done, deadline):
    """Spin until done() or deadline; spin_once drives a single executor."""
    while not done() and time.monotonic() < deadline:
        # rclpy.spin_once runs a SingleThreadedExecutor internally.
        rclpy.spin_once(node, timeout_sec=SPIN_STEP)


def wait_for_match(node, pub, timeout):
    """Wait up to timeout seconds for a matched reader; True when matched."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if pub.get_subscription_count() > 0:
                return True
        except Exception:
            pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        rclpy.spin_once(node, timeout_sec=min(SPIN_STEP, remaining))
    try:
        return pub.get_subscription_count() > 0
    except Exception:
        return False


def finish_recv(args, res, what, done):
    """Log outcome; timeout with zero received exits 2, else exit 0."""
    if done:
        log("%s done %d/%d" % (what, res["received"], args.count))
        return 0
    if res["received"] == 0:
        res["note"] = "timeout with zero received"
    elif not res["note"]:
        res["note"] = "timeout after %.1fs" % args.timeout
    log("%s partial %d/%d" % (what, res["received"], args.count))
    return 2 if res["received"] == 0 else 0


def run_pub(node, args, qos, res):
    """Publish count payloads on topic at rate; always exits 0.

    With --wait-for-match set, waits for a matched reader first and
    exits 2 on timeout since a readerless bench publish is invalid.
    """
    pub = node.create_publisher(String, args.topic, qos)
    if args.wait_for_match and args.wait_for_match > 0:
        if not wait_for_match(node, pub, args.wait_for_match):
            res["note"] = "wait for match timed out after %.1fs" % args.wait_for_match
            log("pub wait for match timed out after %.1fs" % args.wait_for_match)
            return 2
        log("pub matched reader, publishing %d" % args.count)
    start = time.monotonic()
    period = 1.0 / args.rate
    for index in range(args.count):
        delay = start + index * period - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        mark_sent(res, publish_seq(pub, args.seq_base + index, args.size))
    log("pub sent %d/%d" % (res["sent"], args.count))
    return 0


def run_sub(node, args, qos, res):
    """Subscribe until count unique seqs arrive or timeout; 2 when none arrive."""
    track = SeqTrack()

    def on_msg(msg):
        recv_ns = time.time_ns()
        mark_recv(res, recv_ns)
        try:
            send_ns, seq = split_data(msg.data)
        except (struct.error, ValueError, UnicodeEncodeError):
            head = msg.data.encode("utf-8", "replace")[:48]
            if not res["note"]:
                res["note"] = "unparseable payload ignored for seq stats head=%s len=%d" % (head.hex(), len(msg.data))
            return
        mark_sent(res, send_ns)
        track.add(seq)
        res["lat_ns"].append(recv_ns - send_ns)

    node.create_subscription(String, args.topic, on_msg, qos)
    deadline = time.monotonic() + args.timeout
    spin_until(node, lambda: len(track.seen) >= args.count, deadline)
    res["dups"] = track.dups
    res["gaps"] = track.gaps(args.count)
    return finish_recv(args, res, "sub", len(track.seen) >= args.count)


def main(argv=None):
    """Parse, run one mode, print exactly one JSON doc; return exit."""
    args = parse_args(argv if argv is not None else sys.argv[1:])
    qos, qos_names = build_qos(args)
    res = blank_result(args, qos_names)
    rclpy.init()
    try:
        node = make_node(args.mode, args.run_id)
        try:
            if args.mode == "pub":
                code = run_pub(node, args, qos, res)
            else:  # "sub": argparse choices enforce pub|sub only
                code = run_sub(node, args, qos, res)
        finally:
            node.destroy_node()
    finally:
        rclpy.shutdown()
    print(json.dumps(res), flush=True)
    return code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("interrupted")
        sys.exit(130)
    except Exception as exc:  # Crash: nonzero exit, no JSON per contract.
        log("error: %s" % exc)
        sys.exit(1)
