from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    UPLOAD_DIR: Path = BASE_DIR / "uploads"
    OUTPUT_DIR: Path = BASE_DIR / "outputs"

    MAX_FILE_SIZE: int = 62914560
    ALLOWED_EXTENSIONS: set = {".xlsx"}

    ALLOWED_ORIGINS: str = "http://localhost:5173,http://localhost:3000"

    FILE_RETENTION_HOURS: int = 24

    class Config:
        env_file = ".env"

    def allowed_origins_list(self) -> list:
        return [origin.strip() for origin in self.ALLOWED_ORIGINS.split(",") if origin.strip()]


settings = Settings()
settings.UPLOAD_DIR.mkdir(exist_ok=True)
settings.OUTPUT_DIR.mkdir(exist_ok=True)
