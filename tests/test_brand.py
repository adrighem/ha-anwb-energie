"""Tests for Home Assistant brand assets."""

import struct
from pathlib import Path

BRAND_DIR = (
    Path(__file__).parents[1]
    / "custom_components"
    / "anwb_energie_account"
    / "brand"
)
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
MAX_BRAND_FILE_SIZE = 1024 * 1024
BRAND_ICON_DIMENSIONS = {
    "icon.png": (256, 256),
    "icon@2x.png": (512, 512),
}


def _png_size(path: Path) -> tuple[int, int]:
    """Return the dimensions stored in a PNG IHDR chunk."""
    data = path.read_bytes()
    assert data.startswith(PNG_SIGNATURE)
    assert data[12:16] == b"IHDR"
    return struct.unpack(">II", data[16:24])


def test_brand_icon_dimensions() -> None:
    """Brand icons follow the Home Assistant normal and HiDPI sizes."""
    for filename, dimensions in BRAND_ICON_DIMENSIONS.items():
        assert _png_size(BRAND_DIR / filename) == dimensions


def test_brand_icon_file_sizes() -> None:
    """Brand icons remain within the HACS local-brand size limit."""
    for filename in BRAND_ICON_DIMENSIONS:
        path = BRAND_DIR / filename
        assert path.stat().st_size <= MAX_BRAND_FILE_SIZE, (
            f"{filename} exceeds the 1 MiB local-brand size limit"
        )
