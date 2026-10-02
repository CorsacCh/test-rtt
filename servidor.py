import asyncio
import socket
import ssl

from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived


HOST = "0.0.0.0"

TCP_PORT = 5000
UDP_PORT = 5001
QUIC_PORT = 5002


# ============================================================
# TCP
# ============================================================

async def tcp_server():
    async def handle_client(reader, writer):
        while True:
            data = await reader.read(4096)

            if not data:
                break

            print(f"[TCP] Recibido: {data.decode()}")
            writer.write(data)
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(
        handle_client,
        HOST,
        TCP_PORT
    )

    print(f"[TCP] Servidor escuchando en {TCP_PORT}")

    async with server:
        await server.serve_forever()


# ============================================================
# UDP
# ============================================================

class UDPProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport
        print(f"[UDP] Servidor escuchando en {UDP_PORT}")

    def datagram_received(self, data, addr):
        print(f"[UDP] Recibido de {addr}: {data.decode()}")

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

            print(f"[QUIC] Recibido: {event.data.decode()}")

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

    print(f"[QUIC] Servidor escuchando en {QUIC_PORT}")

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

    await asyncio.gather(
        tcp_server(),
        udp_server(),
        quic_server()
    )


if __name__ == "__main__":
    asyncio.run(main())