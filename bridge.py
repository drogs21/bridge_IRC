#!/usr/bin/env python3
"""
IRC Bridge bidirezionale:
  Server 1 (#monitorIRC) <-> Server 2 (#koro)

Standard library only. IPv6 connections, reconnect, JOIN readiness,
message queue, and bridge-loop protection.
"""

import socket
import threading
import time
from collections import deque

# ---------------- CONFIGURAZIONE ----------------

IRC1_HOST = "IRC_SERVER _1"
IRC1_PORT = 6667
IRC1_CHANNEL = "#canale 1"

LOCAL_IPV6 = "CLASSE_IPv6"

IRC2_HOST = "IRC_SERVER_2"
IRC2_PORT = 6667
IRC2_CHANNEL = "#canale 2"

IRC_NICK = "koroIRC"
IRC_USER = "koroBridge"
IRC_REALNAME = "Ti c0dd0 a pRessione"
RECONNECT_DELAY = 5
MAX_PENDING_MESSAGES = 500

# ------------------------------------------------

connections = {"server1": None, "server2": None}
connections_lock = threading.Lock()

pending = {"server1": deque(), "server2": deque()}
pending_lock = threading.Lock()


class IRCConnection:
    def __init__(self, host, port, label, local_ipv6=None, key=None):
        self.host = host
        self.port = port
        self.label = label
        self.local_ipv6 = local_ipv6
        self.key = key
        self.sock = None
        self.send_lock = threading.Lock()
        self.joined_channels = set()
        self.joined_event = threading.Event()
        self.actual_nick = IRC_NICK

    def connect(self):
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.settimeout(30)

        if self.local_ipv6:
            sock.bind((self.local_ipv6, 0, 0, 0))

        sock.connect((self.host, self.port, 0, 0))
        sock.settimeout(None)
        self.sock = sock

        self.send_raw(f"NICK {IRC_NICK}")
        self.send_raw(f"USER {IRC_USER} 0 * :{IRC_REALNAME}")
        print(f"[{self.label}] TCP connesso a [{self.host}]:{self.port}")

    def send_raw(self, line):
        if not self.sock:
            raise ConnectionError("Socket IRC non connesso")

        payload = (line + "\r\n").encode("utf-8", errors="replace")
        with self.send_lock:
            self.sock.sendall(payload)

    def join(self, channel):
        self.send_raw(f"JOIN {channel}")

    def close(self):
        self.joined_event.clear()
        self.joined_channels.clear()
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None


def parse_irc_line(line):
    prefix = None

    if line.startswith(":"):
        if " " not in line:
            return line[1:], "", []
        prefix, line = line[1:].split(" ", 1)

    parts = line.split(" ", 1)
    command = parts[0].upper()
    rest = parts[1] if len(parts) > 1 else ""
    params = []

    while rest:
        if rest.startswith(":"):
            params.append(rest[1:])
            break
        if " " in rest:
            item, rest = rest.split(" ", 1)
            params.append(item)
        else:
            params.append(rest)
            break

    return prefix, command, params


def iter_irc_lines(sock):
    buffer = b""
    while True:
        data = sock.recv(4096)
        if not data:
            raise ConnectionError("Il server ha chiuso la connessione")
        buffer += data

        while b"\r\n" in buffer:
            raw, buffer = buffer.split(b"\r\n", 1)
            yield raw.decode("utf-8", errors="replace")


def is_joined(conn, channel):
    return any(ch.lower() == channel.lower()
               for ch in conn.joined_channels)


def queue_message(peer_key, line):
    with pending_lock:
        queue = pending[peer_key]
        if len(queue) >= MAX_PENDING_MESSAGES:
            queue.popleft()
            print(f"[{peer_key}] Coda piena: eliminato il messaggio più vecchio")
        queue.append(line)


def send_to(peer_key, peer_channel, line):
    with connections_lock:
        peer = connections.get(peer_key)

    # Do not attempt channel messages until destination JOIN is confirmed.
    if peer and peer.sock and is_joined(peer, peer_channel):
        try:
            peer.send_raw(line)
            print(f"[BRIDGE] Inoltrato verso {peer.label}: {line}")
            return
        except OSError as exc:
            print(f"[{peer_key}] Invio non riuscito: {exc}")

    queue_message(peer_key, line)
    print(f"[BRIDGE] Destinazione {peer_key} non pronta; messaggio messo in coda")


def flush_queue(key, conn, channel):
    if not is_joined(conn, channel):
        return

    while True:
        with pending_lock:
            if not pending[key]:
                return
            line = pending[key].popleft()

        try:
            conn.send_raw(line)
            print(f"[BRIDGE] Inviato messaggio in coda a {conn.label}: {line}")
        except OSError as exc:
            print(f"[{conn.label}] Invio della coda fallito: {exc}")
            queue_message(key, line)
            return


def bridge_nick_set():
    return {
        IRC_NICK.lower(),
        f"{IRC_NICK}_1".lower(),
        f"{IRC_NICK}_2".lower(),
    }


def run_server(key, host, port, channel, label, peer_key, peer_channel):
    local_ipv6 = LOCAL_IPV6

    while True:
        conn = IRCConnection(
            host, port, label,
            local_ipv6=local_ipv6,
            key=key,
        )

        try:
            conn.connect()
            with connections_lock:
                connections[key] = conn

            for raw_line in iter_irc_lines(conn.sock):
                print(f"[{label}] {raw_line}")
                prefix, command, params = parse_irc_line(raw_line)

                if command == "PING" and params:
                    conn.send_raw(f"PONG :{params[-1]}")
                    continue

                if command == "001":
                    # 001 prefix is the server name, not our nickname.
                    conn.join(channel)
                    continue

                if command == "433":
                    alternate = f"{IRC_NICK}_{key[-1]}"
                    conn.actual_nick = alternate
                    conn.send_raw(f"NICK {alternate}")
                    continue

                if command == "JOIN" and params:
                    joined_channel = params[0]
                    nick = prefix.split("!", 1)[0] if prefix else ""

                    if nick.lower() == conn.actual_nick.lower():
                        conn.joined_channels.add(joined_channel)
                        print(f"[{label}] JOIN confermato: {joined_channel}")
                        conn.joined_event.set()
                        flush_queue(key, conn, channel)
                    continue

                # Numerico 366 = fine della lista NAMES; useful as
                # an additional indication that the channel JOIN completed.
                if command == "366" and len(params) >= 2:
                    listed_channel = params[1]
                    if listed_channel.lower() == channel.lower():
                        conn.joined_channels.add(listed_channel)
                        conn.joined_event.set()
                        flush_queue(key, conn, channel)
                    continue

                if command == "PART" and params:
                    parted_channel = params[0]
                    nick = prefix.split("!", 1)[0] if prefix else ""
                    if nick.lower() == conn.actual_nick.lower():
                        conn.joined_channels.discard(parted_channel)
                        conn.joined_event.clear()
                    continue

                if command == "KICK" and len(params) >= 2:
                    kicked_channel, kicked_nick = params[0], params[1]
                    if kicked_nick.lower() == conn.actual_nick.lower():
                        conn.joined_channels.discard(kicked_channel)
                        conn.joined_event.clear()
                    continue

                if command != "PRIVMSG" or len(params) < 2:
                    continue

                target, message = params[0], params[1]
                if target.lower() != channel.lower():
                    continue

                nick = prefix.split("!", 1)[0] if prefix else "unknown"

                # Avoid relaying messages sent by either bridge identity.
                if nick.lower() in bridge_nick_set():
                    continue

                outgoing = f"PRIVMSG {peer_channel} :{message}"
                send_to(peer_key, peer_channel, outgoing)

        except (OSError, ConnectionError) as exc:
            print(f"[{label}] Disconnesso: {exc}. "
                  f"Riprovo tra {RECONNECT_DELAY}s.")
        finally:
            conn.close()
            with connections_lock:
                if connections.get(key) is conn:
                    connections[key] = None

        time.sleep(RECONNECT_DELAY)


def main():
    print("IRC Bridge BIDIREZIONALE avviato")
    print(f"Server 1: {IRC1_HOST}:{IRC1_PORT} {IRC1_CHANNEL}")
    print(f"Server 2: [{IRC2_HOST}]:{IRC2_PORT} {IRC2_CHANNEL}")
    print(f"IPv6 locale: {LOCAL_IPV6}")
    print("Direzione: Server 1 <-> Server 2")

    thread1 = threading.Thread(
        target=run_server,
        args=("server1", IRC1_HOST, IRC1_PORT, IRC1_CHANNEL,
              "SERVER 1", "server2", IRC2_CHANNEL),
        daemon=True,
    )
    thread2 = threading.Thread(
        target=run_server,
        args=("server2", IRC2_HOST, IRC2_PORT, IRC2_CHANNEL,
              "SERVER 2", "server1", IRC1_CHANNEL),
        daemon=True,
    )

    thread1.start()
    thread2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nIRC Bridge fermato.")
        with connections_lock:
            active = list(connections.values())
        for conn in active:
            if conn:
                conn.close()


if __name__ == "__main__":
    main()
