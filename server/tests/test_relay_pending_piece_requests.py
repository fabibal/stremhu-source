import asyncio
from typing import cast

import libtorrent

from app.modules.relay.service import RelayService

INFO_HASH = "a" * 40
PIECE_INDEX = 7
KEY = (INFO_HASH, PIECE_INDEX)


class FakeHandle:
    """Minimális torrent_handle: a darab megvan, a read_piece csak számolja a hívásokat."""

    def __init__(self) -> None:
        self.read_calls = 0

    def info_hash(self) -> str:
        return INFO_HASH

    def have_piece(self, piece_index: int) -> bool:
        _ = piece_index
        return True

    def read_piece(self, piece_index: int) -> None:
        _ = piece_index
        self.read_calls += 1


def make_service(loop: asyncio.AbstractEventLoop) -> RelayService:
    # A teljes konstruktor libtorrent sessiont indítana, a get_piece_data-hoz
    # viszont csak ez a két mező kell.
    service = object.__new__(RelayService)
    service.loop = loop
    service.pending_piece_requests = {}
    return service


def deliver_read_piece(service: RelayService, data: bytes) -> None:
    """A process_alerts read_piece_alert ágát utánozza.

    A várakozó listát kiveszi a szótárból, a futurek eredményét pedig
    call_soon_threadsafe-fel állítja be, vagyis a várakozók csak később ébrednek.
    """
    futures = service.pending_piece_requests.pop(KEY, None)
    for future in futures or []:
        if not future.done():
            service.loop.call_soon_threadsafe(future.set_result, data)


def test_newer_waiter_keeps_its_pending_list_when_older_one_finishes():
    """Ha az alert után, de a korábbi kérés felébredése előtt érkezik egy második
    kérés ugyanarra a darabra, a korábbi kérés takarítása nem törölheti a második
    várakozási listáját. Különben a második soha nem kap választ."""

    async def scenario() -> None:
        service = make_service(asyncio.get_running_loop())
        handle = cast(libtorrent.torrent_handle, FakeHandle())

        first = asyncio.create_task(service.get_piece_data(handle, PIECE_INDEX))
        await asyncio.sleep(0)

        # Az alert feldolgozása: a lista kikerül a szótárból, az első kérés
        # eredménye viszont csak ütemezve van.
        deliver_read_piece(service, b"first")

        # Mielőtt az első kérés folytatódna, megérkezik a második.
        second = asyncio.create_task(service.get_piece_data(handle, PIECE_INDEX))

        assert await first == b"first"

        # A második kérés read_piece_alert-je: az első takarítása után is meg kell
        # találnia a várakozót.
        deliver_read_piece(service, b"second")
        assert await asyncio.wait_for(second, timeout=1) == b"second"
        assert service.pending_piece_requests == {}

    asyncio.run(scenario())
