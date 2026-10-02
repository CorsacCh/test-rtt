import asyncio

from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived


HOST = "172.24.0.1"

TCP_PORT = 5000
UDP_PORT = 5001
QUIC_PORT = 5002


# ============================================================
# TCP
# ============================================================

async def handle_tcp(reader, writer):

    addr = writer.get_extra_info("peername")

    print(f"[TCP] Cliente conectado: {addr}")

    try:
        while True:

            data = await reader.read(4096)

            if not data:
                break

            # Echo
            writer.write(data)
            await writer.drain()

    except ConnectionError:
        pass

    finally:
        writer.close()
        await writer.wait_closed()

        print(f"[TCP] Cliente desconectado: {addr}")


async def tcp_server():

    server = await asyncio.start_server(
        handle_tcp,
        HOST,
        TCP_PORT
    )

    print(f"[TCP] Escuchando en {HOST}:{TCP_PORT}")

    async with server:
        await server.serve_forever()


# ============================================================
# UDP
# ============================================================

class UDPProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):

        self.transport = transport

        print(f"[UDP] Escuchando en {HOST}:{UDP_PORT}")

    def datagram_received(self, data, addr):

        # Echo
        self.transport.sendto(data, addr)


async def udp_server():

    loop = asyncio.get_running_loop()

    await loop.create_datagram_endpoint(
        UDPProtocol,
        local_addr=(HOST, UDP_PORT)
    )

    await asyncio.Future()


# ============================================================
# QUIC
# ============================================================

class QUICProtocol(QuicConnectionProtocol):

    def quic_event_received(self, event):

        if isinstance(event, StreamDataReceived):

            # Echo del mismo stream
            self._quic.send_stream_data(
                event.stream_id,
                event.data,
                end_stream=event.end_stream
            )

            self.transmit()


async def quic_server():

    configuration = QuicConfiguration(
        is_client=False,
        alpn_protocols=["hq-29"]
    )

    configuration.load_cert_chain(
        certfile="cert.pem",
        keyfile="key.pem"
    )

    print(f"[QUIC] Escuchando en {HOST}:{QUIC_PORT}")

    await serve(
        HOST,
        QUIC_PORT,
        configuration=configuration,
        create_protocol=QUICProtocol
    )

    await asyncio.Future()


# ============================================================
# MAIN
# ============================================================

async def main():

    print("=" * 60)
    print("SERVIDOR RTT - TCP / UDP / QUIC")
    print("=" * 60)

    await asyncio.gather(
        tcp_server(),
        udp_server(),
        quic_server()
    )


if __name__ == "__main__":
    asyncio.run(main())