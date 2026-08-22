import time
from typing import Optional
from redis import asyncio as aioredis
from app.core.config import settings
from app.core.logging import logger

# Lua script to perform sliding window rate limiting atomically
LUA_RATE_LIMITER = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local clear_before = now - window

-- Remove old requests
redis.call('zremrangebyscore', key, '-inf', clear_before)

-- Count current requests in window
local current_requests = redis.call('zcard', key)

if current_requests < limit then
    -- Add the request timestamp (use timestamp as score and member)
    -- We append a random identifier to ensure uniqueness under high concurrency
    local unique_member = tostring(now) .. '_' .. tostring(math.random())
    redis.call('zadd', key, now, unique_member)
    -- Set TTL on key to auto-cleanup when inactive
    redis.call('expire', key, window)
    return 0 -- Allowed
else
    return 1 -- Rate limited
end
"""

class RateLimiter:
    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis_url = redis_url
        self.pool = aioredis.ConnectionPool.from_url(redis_url, decode_responses=True)
        self.client = aioredis.Redis(connection_pool=self.pool)
        self._lua_script = None

    async def initialize(self) -> None:
        """Register the Lua script with Redis."""
        try:
            self._lua_script = self.client.register_script(LUA_RATE_LIMITER)
        except Exception as e:
            logger.error("Failed to initialize Redis rate limiter script", error=str(e))

    async def is_rate_limited(self, identifier: str, limit: int = None, window: int = None) -> bool:
        """
        Check if the identifier is rate limited.
        Returns True if rate limited (blocked), False otherwise (allowed).
        """
        if not self._lua_script:
            # If Redis is unavailable or script registration failed, fail open or closed
            # Let's fail open to ensure reliability if Redis is down, but log warning.
            logger.warn("Rate limiter script not registered, failing open")
            return False

        limit = limit or settings.RATE_LIMIT_LIMIT
        window = window or settings.RATE_LIMIT_WINDOW
        key = f"rate_limit:{identifier}"
        now = time.time()

        try:
            # Execute Lua script
            # Returns 0 for allowed, 1 for rate limited
            res = await self._lua_script(keys=[key], args=[now, window, limit])
            return res == 1
        except Exception as e:
            # If Redis goes down, degrade gracefully (fail open) and log the failure
            logger.error("Redis rate limiting error - degrading gracefully (failing open)", error=str(e))
            return False

    async def close(self) -> None:
        """Close connection pool."""
        await self.client.close()
        await self.pool.disconnect()

# Global rate limiter instance
rate_limiter = RateLimiter()
