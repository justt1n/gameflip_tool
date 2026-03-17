import base64
import hashlib
import hmac
import struct
import time

from auth.base_auth import IAuthHandler


class GameflipAuth(IAuthHandler):
    """Generate the `GFAPI key:totp` authorization header."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        period: int = 30,
        digits: int = 6,
        algorithm: str = "SHA1",
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.period = period
        self.digits = digits
        self.algorithm = algorithm.upper()

    def generate_totp(self, for_time: int | None = None) -> str:
        if not self.api_secret:
            raise ValueError("GAMEFLIP_API_SECRET is not configured")
        if self.algorithm != "SHA1":
            raise ValueError(f"Unsupported TOTP algorithm: {self.algorithm}")

        timestamp = int(time.time() if for_time is None else for_time)
        counter = timestamp // self.period
        counter_bytes = struct.pack(">Q", counter)
        secret = self._decode_base32(self.api_secret)
        digest = hmac.new(secret, counter_bytes, hashlib.sha1).digest()
        offset = digest[-1] & 0x0F
        binary = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
        code = binary % (10 ** self.digits)
        return str(code).zfill(self.digits)

    async def get_auth_headers(self) -> dict[str, str]:
        if not self.api_key:
            raise ValueError("GAMEFLIP_API_KEY is not configured")
        return {
            "Authorization": f"GFAPI {self.api_key}:{self.generate_totp()}",
        }

    @staticmethod
    def _decode_base32(value: str) -> bytes:
        normalized = value.strip().replace(" ", "").upper()
        padding = "=" * ((8 - len(normalized) % 8) % 8)
        return base64.b32decode(normalized + padding, casefold=True)
