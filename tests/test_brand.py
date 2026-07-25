"""Tests for Home Assistant brand assets."""

from pathlib import Path
import struct

BRAND_DIR = (
    Path(__file__).parents[1]
    / "custom_components"
    / "anwb_energie_account"
    / "brand"
)
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _png_size(path: Path) -> tuple[int, int]:
    """Return the dimensions stored in a PNG IHDR chunk."""
    data = path.read_bytes()
    assert data.startswith(PNG_SIGNATURE)
    assert data[12:16] == b"IHDR"
    return struct.unpack(">II", data[16:24])


def test_brand_icon_dimensions() -> None:
    """Brand icons follow the Home Assistant normal and HiDPI sizes."""
    assert _png_size(BRAND_DIR / "icon.png") == (256, 256)
    assert _png_size(BRAND_DIR / "icon@2x.png") == (512, 512)
