import pytest

from auth.gameflip_auth import GameflipAuth


class TestGameflipAuth:
    def test_generate_totp_matches_rfc_vector(self):
        auth = GameflipAuth(
            api_key="demo-key",
            api_secret="GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
        )
        assert auth.generate_totp(for_time=59) == "287082"

    @pytest.mark.asyncio
    async def test_auth_header_shape(self):
        auth = GameflipAuth(
            api_key="my-key",
            api_secret="GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
        )
        headers = await auth.get_auth_headers()
        assert headers["Authorization"].startswith("GFAPI my-key:")
