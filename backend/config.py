from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Chúng ta gán giá trị mặc định luôn để nó không đòi file .env nữa
    POSTGRES_URL: str = "sqlite:///./game_data.db"
    DEBUG: bool = False
    API_PREFIX: str = "/api"

    class Config:
        env_file = ".env"


settings = Settings()
