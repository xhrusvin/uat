from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from app.core.config import settings

_client: AsyncIOMotorClient | None = None


async def connect_db():
    """Create Motor client and initialise Beanie ODM."""
    global _client
    from app.models.user import User  # local import avoids circular dep

    _client = AsyncIOMotorClient(settings.MONGODB_URI)
    await init_beanie(
        database=_client[settings.MONGODB_DB],
        document_models=[User],
        allow_index_dropping=False,   # never drop existing indexes
        recreate_views=False,
    )


async def close_db():
    """Close the Motor connection cleanly."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
