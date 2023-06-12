class Config:
    # Example values:
    # - 60: 1 sim minute/second
    # - 600: 10 sim minutes/second (1 sim hour in sim's time is 10 seconds, 1 day is 240 seconds or 4 minutes)
    # - 3600: 1 sim hour per second
    # - 86400: 1 sim day per second
    # - 604800: 1 sim week per second
    CLOCK_MULTIPLIER = 3600

    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_USER = "postgres"
    DB_PASS = "12345678"
    DB_NAME = "postgres"

    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Connection to MSS main application
    MSS_HOST = "localhost"
    MSS_PORT = "8080"

    # Pre-generate test data
    DROP_DB_ON_LAUNCH = True
    WARMUP_ENABLED = True
    WARMUP_NUM_OF_ARTISTS = 100
    WARMUP_NUM_OF_COUNTRIES = 10
    WARMUP_NUM_OF_COUNTRIES_ENABLED = 5
    WARMUP_NUM_OF_USERS = 50

    # Simulation statistics

    STAT_USER_CONTROLLER = {
        "prob_user_age_mean": 28,
        "prob_user_age_sigma": 12,
        "prob_user_age_min": 16,
        "prob_user_age_max": 100,
        "prob_country_weight_half_life": 14,  # in days
        "avg_users_per_day_per_country": 3,
        "delay_create_users_sec": 60 * 60,
        "delay_select_users_sec": 60,
        "delay_run_users_sec": 70,
        "delay_clean_up_users_sec": 120,
    }

    STAT_USER = {
        "delay_between_states_sec": 10,
        "prob_leave_session": 0.1,
        "prob_subscription": 0.01,
    }

    STAT_ARTIST_CONTROLLER = {
        "prob_new_genre": 0.03,
        "prob_same_country_genre": 0.8,
        "prob_derived_genre_same_country": 0.02,
        "prob_derived_genre_diff_country": 0.1,
        "prob_retired": 0.1,
        "prob_col_new_genre": 0.1,
        "prob_col_invent_genre": 0.03,
        "delay_create_artist_sec": 60 * 60 * 24,
        "delay_select_artist_sec": 60 * 60 * 24,
        "delay_run_artist_sec": 60 * 60 * 5,
    }

    ENABLE_COUNTRY_DELAY_DAYS = 30
