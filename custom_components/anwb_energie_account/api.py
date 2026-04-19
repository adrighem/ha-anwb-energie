"""API for ANWB Energie Account bound to Home Assistant OAuth."""

from typing import cast

from aiohttp import ClientSession

from homeassistant.helpers import config_entry_oauth2_flow


class AsyncConfigEntryAuth:
    """Provide ANWB Energie Account authentication tied to an OAuth2 based config entry."""

    def __init__(
        self,
        websession: ClientSession,
        oauth_session: config_entry_oauth2_flow.OAuth2Session,
    ) -> None:
        """Initialize ANWB Energie Account auth."""
        self.websession = websession
        self._oauth_session = oauth_session

    async def async_get_access_token(self) -> str:
        """Return a valid access token."""
        await self._oauth_session.async_ensure_token_valid()

        return cast(str, self._oauth_session.token["access_token"])
