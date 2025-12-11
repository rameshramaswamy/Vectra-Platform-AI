from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

# 1. Ensure URL uses the Async Driver
# SQLAlchemy needs 'postgresql+asyncpg://', but standard URL is usually 'postgresql://'
ASYNC_DATABASE_URL = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# 2. Create Async Engine
# This engine is non-blocking and uses the optimized asyncpg driver under the hood.
engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=False,           # Set True for SQL debugging
    pool_size=20,         # Enterprise: Larger pool for high concurrency
    max_overflow=10,      # Allow burst
    pool_timeout=30,
    pool_pre_ping=True,   # Resilience: Checks connection health before yielding
    future=True
)

# 3. Async Session Factory
# Use this when you need ORM capabilities (e.g., querying for metadata)
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)

# 4. Dependency Injection Helper (for FastAPI or context managers)
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()