import asyncio
import socket
import statistics
import time

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived


# ============================================================
# CONFIGURACIÓN
# ============================================================

SERVER = "0.0.0.0"  # <-- CAMBIA ESTO POR LA IP DEL SERVIDOR

TCP_PORT = 5000
UDP_PORT = 5001
QUIC_PORT = 5002

MESSAGE = b"RTT_TEST"

NUM_TESTS = 1000

# Pruebas realizadas antes de medir
WARMUP_TESTS = 10

TIMEOUT = 2.0


# ============================================================
# UTILIDADES
# ============================================================

def calculate_statistics(times):

    if not times:
        return None

    sorted_times = sorted(times)

    return {
        "count": len(times),
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "std": statistics.stdev(times) if len(times) > 1 else 0,
        "p95": sorted_times[
            int(len(sorted_times) * 0.95) - 1
        ]
    }


def print_statistics(protocol, times, failed):

    stats = calculate_statistics(times)

    print()
    print("=" * 60)
    print(protocol)
    print("=" * 60)

    if stats is None:

        print("No se obtuvieron respuestas.")

        return

    print(f"Pruebas exitosas : {stats['count']}")
    print(f"Pruebas fallidas : {failed}")

    print(f"RTT medio        : {stats['mean']:.3f} ms")
    print(f"RTT mediano      : {stats['median']:.3f} ms")
    print(f"RTT mínimo       : {stats['min']:.3f} ms")
    print(f"RTT máximo       : {stats['max']:.3f} ms")
    print(f"Desv. estándar   : {stats['std']:.3f} ms")
    print(f"P95              : {stats['p95']:.3f} ms")

    total = stats["count"] + failed

    if total > 0:

        loss = (failed / total) * 100

        print(f"Pérdida          : {loss:.2f} %")


# ============================================================
# TCP
# ============================================================

def test_tcp():

    print("\nConectando mediante TCP...")

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    sock.settimeout(TIMEOUT)

    sock.connect(
        (SERVER, TCP_PORT)
    )

    print("Conexión TCP establecida.")

    # --------------------------------------------------------
    # Warm-up
    # --------------------------------------------------------

    print(f"Ejecutando {WARMUP_TESTS} pruebas de calentamiento...")

    for _ in range(WARMUP_TESTS):

        sock.sendall(MESSAGE)

        received = b""

        while len(received) < len(MESSAGE):

            chunk = sock.recv(
                len(MESSAGE) - len(received)
            )

            if not chunk:
                raise ConnectionError(
                    "El servidor cerró la conexión TCP."
                )

            received += chunk

    # --------------------------------------------------------
    # Mediciones
    # --------------------------------------------------------

    print(f"Ejecutando {NUM_TESTS} mediciones TCP...")

    times = []
    failed = 0

    for _ in range(NUM_TESTS):

        try:

            start = time.perf_counter_ns()

            sock.sendall(MESSAGE)

            received = b""

            while len(received) < len(MESSAGE):

                chunk = sock.recv(
                    len(MESSAGE) - len(received)
                )

                if not chunk:
                    raise ConnectionError(
                        "Conexión TCP cerrada."
                    )

                received += chunk

            end = time.perf_counter_ns()

            if received != MESSAGE:
                failed += 1
                continue

            rtt_ms = (end - start) / 1_000_000

            times.append(rtt_ms)

        except (socket.timeout, ConnectionError):

            failed += 1

    sock.close()

    return times, failed


# ============================================================
# UDP
# ============================================================

def test_udp():

    print("\nPreparando UDP...")

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    sock.settimeout(TIMEOUT)

    server_addr = (
        SERVER,
        UDP_PORT
    )

    # --------------------------------------------------------
    # Warm-up
    # --------------------------------------------------------

    print(f"Ejecutando {WARMUP_TESTS} pruebas de calentamiento...")

    for _ in range(WARMUP_TESTS):

        try:

            sock.sendto(
                MESSAGE,
                server_addr
            )

            sock.recvfrom(4096)

        except socket.timeout:
            pass

    # --------------------------------------------------------
    # Mediciones
    # --------------------------------------------------------

    print(f"Ejecutando {NUM_TESTS} mediciones UDP...")

    times = []
    failed = 0

    for _ in range(NUM_TESTS):

        try:

            start = time.perf_counter_ns()

            sock.sendto(
                MESSAGE,
                server_addr
            )

            response, addr = sock.recvfrom(4096)

            end = time.perf_counter_ns()

            if response != MESSAGE:
                failed += 1
                continue

            rtt_ms = (end - start) / 1_000_000

            times.append(rtt_ms)

        except socket.timeout:

            failed += 1

    sock.close()

    return times, failed


# ============================================================
# QUIC
# ============================================================

class QUICClientProtocol(QuicConnectionProtocol):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.pending = {}

    def quic_event_received(self, event):

        if isinstance(event, StreamDataReceived):

            stream_id = event.stream_id

            future = self.pending.get(stream_id)

            if future is not None:

                if not future.done():

                    future.set_result(event.data)

                del self.pending[stream_id]


    async def send_message(self, message):

        stream_id = self._quic.get_next_available_stream_id()

        loop = asyncio.get_running_loop()

        future = loop.create_future()

        self.pending[stream_id] = future

        self._quic.send_stream_data(
            stream_id,
            message,
            end_stream=True
        )

        self.transmit()

        try:

            response = await asyncio.wait_for(
                future,
                timeout=TIMEOUT
            )

            return response

        except asyncio.TimeoutError:

            self.pending.pop(
                stream_id,
                None
            )

            raise


async def test_quic():

    print("\nEstableciendo conexión QUIC...")

    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=["hq-29"]
    )

    # Certificado autofirmado
    configuration.verify_mode = False

    times = []
    failed = 0

    async with connect(
        SERVER,
        QUIC_PORT,
        configuration=configuration,
        create_protocol=QUICClientProtocol
    ) as protocol:

        print("Handshake QUIC completado.")

        # ----------------------------------------------------
        # Warm-up
        # ----------------------------------------------------

        print(
            f"Ejecutando {WARMUP_TESTS} "
            "pruebas de calentamiento..."
        )

        for _ in range(WARMUP_TESTS):

            try:

                await protocol.send_message(
                    MESSAGE
                )

            except asyncio.TimeoutError:

                pass

        # ----------------------------------------------------
        # Mediciones
        # ----------------------------------------------------

        print(
            f"Ejecutando {NUM_TESTS} mediciones QUIC..."
        )

        for _ in range(NUM_TESTS):

            try:

                start = time.perf_counter_ns()

                response = await protocol.send_message(
                    MESSAGE
                )

                end = time.perf_counter_ns()

                if response != MESSAGE:

                    failed += 1
                    continue

                rtt_ms = (end - start) / 1_000_000

                times.append(rtt_ms)

            except asyncio.TimeoutError:

                failed += 1

    return times, failed


# ============================================================
# MAIN
# ============================================================

async def main():

    print()
    print("=" * 60)
    print("EXPERIMENTO DE RTT")
    print("TCP vs UDP vs QUIC")
    print("=" * 60)

    print()
    print(f"Servidor       : {SERVER}")
    print(f"Mediciones     : {NUM_TESTS}")
    print(f"Warm-up        : {WARMUP_TESTS}")
    print(f"Timeout        : {TIMEOUT} s")

    # --------------------------------------------------------
    # TCP
    # --------------------------------------------------------

    tcp_times, tcp_failed = test_tcp()

    print_statistics(
        "TCP",
        tcp_times,
        tcp_failed
    )

    # --------------------------------------------------------
    # UDP
    # --------------------------------------------------------

    udp_times, udp_failed = test_udp()

    print_statistics(
        "UDP",
        udp_times,
        udp_failed
    )

    # --------------------------------------------------------
    # QUIC
    # --------------------------------------------------------

    quic_times, quic_failed = await test_quic()

    print_statistics(
        "QUIC",
        quic_times,
        quic_failed
    )

    # --------------------------------------------------------
    # RESUMEN
    # --------------------------------------------------------

    print()
    print("=" * 60)
    print("RESUMEN DE RTT")
    print("=" * 60)

    protocols = [
        ("TCP", tcp_times),
        ("UDP", udp_times),
        ("QUIC", quic_times)
    ]

    for name, times in protocols:

        if times:

            average = statistics.mean(times)

            print(
                f"{name:<6} "
                f"media = {average:.3f} ms"
            )

        else:

            print(
                f"{name:<6} "
                "sin resultados"
            )


if __name__ == "__main__":
    asyncio.run(main())