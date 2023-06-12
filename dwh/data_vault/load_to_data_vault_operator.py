from sqlalchemy.sql import text
from sqlalchemy import select

from datetime import datetime
from random import uniform
from ..stage.models import KafkaCountryEnabled

from .. import engine


class Metadata:
    def __init__(self, metadata: dict) -> None:
        self.process_name = metadata["process_name"]
        self.target_schema = metadata["target"]["schema"]
        self.target_table = metadata["target"]["table"]
        self.target_key = metadata["target"]["key"]
        self.target_fields = metadata["target"]["fields"]

        self.source_table = metadata["source"]["table"]
        self.source_fields = metadata["source"]["fields"]
        self.source_schema = metadata["source"]["schema"]
        self.source_name = metadata["source"]["name"]
        self.source_key = metadata["source"]["key"]

        self.custom_query = metadata["custom_query"]


def generate_hash_key(source_key: str, source_name: str) -> str:
    return f"""encode(digest(concat_ws('#', {source_key}) || '#' 
                || '{source_name}', 'sha256'),'base64') 
                """


def load_to_hub(metadata: Metadata) -> None:
    with engine.connect() as conn:
        query = text(
            f"""insert into {metadata.target_schema}.{metadata.target_table} ({metadata.target_key}, original_id, source)
                select {generate_hash_key(metadata.source_key, metadata.source_name)}
                                as {metadata.target_key},
                    {metadata.source_key} as original_id,
                    '{metadata.source_name}' as source
                from {metadata.source_schema}.{metadata.source_table}
                where {metadata.source_key} not in 
                    (select original_id from {metadata.target_schema}.{metadata.target_table});"""
        )
        print(query)
        conn.execute(query)
        conn.commit()


def load_to_link(metadata: Metadata) -> None:
    with engine.connect() as conn:
        separated_target_fields = metadata.target_fields.split(",")
        separated_source_fields = metadata.source_fields.split(",")
        hashed_fields = ""
        for i in range(len(separated_source_fields)):
            hashed_fields += f"""{generate_hash_key(separated_source_fields[i], metadata.source_name)}
                                as {separated_target_fields[i]},"""
        hashed_fields = hashed_fields[:-1]
        hashed_key = generate_hash_key(metadata.source_fields, metadata.source_name)

        query = text(
            f"""insert into {metadata.target_schema}.{metadata.target_table}
            ({metadata.target_key}, {metadata.target_fields})
                select {hashed_key} as {metadata.target_key},
                        {hashed_fields}
                from ({metadata.custom_query}) t
                where {hashed_key}
                    not in (select {metadata.target_key} from {metadata.target_schema}.{metadata.target_table})"""
        )
        conn.execute(query)
        conn.commit()


def load_to_lsat(metadata: Metadata) -> None:
    with engine.connect() as conn:
        hashed_key = generate_hash_key(metadata.source_key, metadata.source_name)
        query = text(
            f"""insert into {metadata.target_schema}.{metadata.target_table}
                    ({metadata.target_key}, {metadata.target_fields}, source)
                    with source_data as({metadata.custom_query})
                    select {hashed_key} as {metadata.target_key},
                        {metadata.target_fields},
                        '{metadata.source_name}' as source
                    from source_data
                    where {hashed_key} || actual_dtm::varchar not in (select {metadata.target_key} || actual_dtm::varchar
                                            from {metadata.target_schema}.{metadata.target_table})"""
        )
        conn.execute(query)
        conn.commit()


def load_to_sat(metadata: Metadata):
    with engine.connect() as conn:
        hashed_key = generate_hash_key(
            "sd." + metadata.target_key, metadata.source_name
        )

        target_fields = metadata.target_fields.split(",")
        coalesce_target_fields_as = ""
        for field in target_fields:
            coalesce_target_fields_as += f"coalesce(sd.{field}, pd.{field}) as {field},"
        coalesce_target_fields_as = coalesce_target_fields_as[:-1]

        coalesce_target_fields = ""
        for field in target_fields:
            coalesce_target_fields += f"coalesce(sd.{field}, pd.{field}),"
        coalesce_target_fields = coalesce_target_fields[:-1]

        row_hash = generate_hash_key(coalesce_target_fields, metadata.source_name)

        query = text(
            f"""insert into {metadata.target_schema}.{metadata.target_table}
        ({metadata.target_key}, {metadata.target_fields}, row_hash, source)
        with source_data as ({metadata.custom_query}),

        previous_data as (select *,
                        row_number() over (partition by {metadata.target_key} order by actual_dtm desc) as rnum
                        from {metadata.target_schema}.{metadata.target_table}),
        
        hashed as (select {hashed_key} as {metadata.target_key},
                          {coalesce_target_fields_as},
                          {row_hash} as row_hash,
                          '{metadata.source_name}' as source
                   from source_data sd
                    left join previous_data pd 
				  	       on sd.{metadata.target_key} = pd.{metadata.target_key}
					      and pd.rnum = 1)
           
        select * from hashed where row_hash not in (select row_hash 
            from {metadata.target_schema}.{metadata.target_table})"""
        )
        conn.execute(query)
        conn.commit()


def load_to_ref(metadata: Metadata):
    if metadata.process_name == 'ref_country_subscription_cost':
        load_countries(metadata)
    elif metadata.process_name == 'ref_genre':
        load_genres(metadata)



cost_range = {
    "basic_subscription": (8, 15),
    "premium_multiplier": (1.1, 1.6),
    "royalty_cost": (0.01, 0.1),
    "premium_royalty_multiplier": 1.2,
}


def generate_values_ref_country():
    basic_subscription = round(
        uniform(
            cost_range["basic_subscription"][0], cost_range["basic_subscription"][1]
        ),
        2,
    )
    premium_multiplier = uniform(
        cost_range["premium_multiplier"][0], cost_range["premium_multiplier"][1]
    )
    premium_subscription = round(basic_subscription * premium_multiplier, 2)
    royalty = round(
        uniform(cost_range["royalty_cost"][0], cost_range["royalty_cost"][1]), 2
    )
    premium_royalty = round(royalty * cost_range["premium_royalty_multiplier"], 2)
    return basic_subscription, premium_subscription, royalty, premium_royalty

def load_countries(metadata: Metadata):
    stmt = select(KafkaCountryEnabled.country_code, KafkaCountryEnabled.event_time)
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    for country in results:
        country_code = country[0]
        enabled_dt = country[1]
        basic_subscription, premium_subscription, royalty, premium_royalty = generate_values_ref_country()
        with engine.connect() as conn:
            query = text(f"""insert into {metadata.target_schema}.{metadata.target_table}
        ({metadata.target_key}, {metadata.target_fields}, source) VALUES
        ('{country_code}', '{enabled_dt}', 'Basic', {basic_subscription}, {royalty}, '{metadata.source_name}'),
        ('{country_code}', '{enabled_dt}', 'Premium', {premium_subscription}, {premium_royalty}, '{metadata.source_name}')""")
            conn.execute(query)
            conn.commit()
            
def load_genres(metadata: Metadata):
    with engine.connect() as conn:
            query = text(f"""insert into {metadata.target_schema}.{metadata.target_table}
        ({metadata.target_fields})
        {metadata.custom_query}
        """)
            conn.execute(query)
            conn.commit()

