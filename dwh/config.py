class Config:

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
    
    EXECUTION_DATE_MART = '2023-05-26'




