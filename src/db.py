import os
from sqlalchemy import create_engine


def get_source_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('SRC_POSTGRES_USER')}:{os.getenv('SRC_POSTGRES_PASSWORD')}"
        f"@{os.getenv('SRC_POSTGRES_HOST')}:{os.getenv('SRC_POSTGRES_PORT')}/{os.getenv('SRC_POSTGRES_DB')}"
    )


def get_target_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('TGT_POSTGRES_USER')}:{os.getenv('TGT_POSTGRES_PASSWORD')}"
        f"@{os.getenv('TGT_POSTGRES_HOST')}:{os.getenv('TGT_POSTGRES_PORT')}/{os.getenv('TGT_POSTGRES_DB')}"
    )