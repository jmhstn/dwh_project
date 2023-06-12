class Config:
    JWT_SECRET_KEY = "f2405682e2a614454e8d35f4b88b661dcdc1ae0f"

    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_USER = "postgres"
    DB_PASS = "12345678"
    DB_NAME = "postgres"

    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
    KAFKA_DWH_TOPIC = "dwh_events"

    DROP_DB_ON_START = True

    SERVER_HOST = "0.0.0.0"
    SERVER_PORT = 8080
