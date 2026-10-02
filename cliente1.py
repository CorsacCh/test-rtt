import asyncio
import socket
import time

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived


SERVER = "127.0.0.1"

TCP_PORT = 5000
UDP_PORT = 5001
QUIC_PORT = 5002

MESSAGE = b"Hola desde Python"


# ============================================================
# TCP
# ============================================================

def test_tcp():

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    sock.connect((SERVER, TCP_PORT))

    start = time.perf_counter()

    sock.sendall(MESSAGE)

    response = sock.recv(4096)

    end = time.perf_counter()

    sock.close()

    rtt = (end - start) * 1000

    print("\n[TCP]")
    print("Mensaje enviado:", MESSAGE.decode())
    print("Respuesta:", response.decode())
    print(f"RTT: {rtt:.3f} ms")

    return rtt


# ============================================================
# UDP
# ============================================================

def test_udp():

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    sock.settimeout(5)

    start = time.perf_counter()

    sock.sendto(
        MESSAGE,
        (SERVER, UDP_PORT)
    )

    response, _ = sock.recvfrom(4096)

    end = time.perf_counter()

    sock.close()

    rtt = (end - start) * 1000

    print("\n[UDP]")
    print("Mensaje enviado:", MESSAGE.decode())
    print("Respuesta:", response.decode())
    print(f"RTT: {rtt:.3f} ms")

    return rtt


# ============================================================
# QUIC
# ============================================================

class ClientProtocol(QuicConnectionProtocol):

    def __init__(self, *args, message=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.message = message
        self.response = None

        self.finished = asyncio.Event()

    def quic_event_received(self, event):

        if isinstance(event, StreamDataReceived):

            self.response = event.data

            if event.end_stream:
                self.finished.set()


async def test_quic():

    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=["hq-29"]
    )

    # Para pruebas locales.
    configuration.verify_mode = False

    start = time.perf_counter()

    async with connect(
        SERVER,
        QUIC_PORT,
        configuration=configuration,
        create_protocol=lambda *args, **kwargs:
            ClientProtocol(
                *args,
                message=MESSAGE,
                **kwargs
            )
    ) as protocol:

        stream_id = protocol._quic.get_next_available_stream_id()

        protocol._quic.send_stream_data(
            stream_id,
            MESSAGE,
            end_stream=True
        )

        protocol.transmit()

        await protocol.finished.wait()

        end = time.perf_counter()

        response = protocol.response

    rtt = (end - start) * 1000

    print("\n[QUIC]")
    print("Mensaje enviado:", MESSAGE.decode())
    print("Respuesta:", response.decode())
    print(f"RTT: {rtt:.3f} ms")

    return rtt


# ============================================================
# MAIN
# ============================================================

async def main():

    print("=" * 50)
    print("PRUEBA TCP / UDP / QUIC")
    print("=" * 50)

    tcp_time = test_tcp()

    udp_time = test_udp()

    quic_time = await test_quic()

    print("\n" + "=" * 50)
    print("RESULTADOS")
    print("=" * 50)

    print(f"TCP  : {tcp_time:.3f} ms")
    print(f"UDP  : {udp_time:.3f} ms")
    print(f"QUIC : {quic_time:.3f} ms")


if __name__ == "__main__":
    asyncio.run(main())