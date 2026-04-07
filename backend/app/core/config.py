from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    UPLOAD_DIR: Path = BASE_DIR / "uploads"
    OUTPUT_DIR: Path = BASE_DIR / "outputs"

    MAX_FILE_SIZE_MB: int = 200
    ALLOWED_EXTENSIONS: set = {".xlsx", ".csv"}

    ALLOWED_ORIGINS: str = "http://localhost:5173,http://localhost:3000"

    FILE_RETENTION_HOURS: int = 24

    class Config:
        env_file = ".env"

    @property
    def max_file_size_bytes(self) -> int:
        return self.MAX_FILE_SIZE_MB * 1024 * 1024

    def allowed_origins_list(self) -> list:
        return [origin.strip() for origin in self.ALLOWED_ORIGINS.split(",") if origin.strip()]


settings = Settings()
settings.UPLOAD_DIR.mkdir(exist_ok=True)
settings.OUTPUT_DIR.mkdir(exist_ok=True)
