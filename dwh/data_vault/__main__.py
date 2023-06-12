import yaml

from .models import meta, engine
from .load_to_data_vault_operator import (
    Metadata,
    load_to_hub,
    load_to_link,
    load_to_lsat,
    load_to_sat,
    load_to_ref,
)


meta.create_all(engine)


with open("dwh/data_vault/metadata/hub.yaml", "r") as file:
    process_metadata = yaml.safe_load(file)
    for process in process_metadata:
        metadata = Metadata(process)
        load_to_hub(metadata)

with open("dwh/data_vault/metadata/sat.yaml", "r") as file:
    process_metadata = yaml.safe_load(file)
    for process in process_metadata:
        metadata = Metadata(process)
        load_to_sat(metadata)

with open("dwh/data_vault/metadata/link.yaml", "r") as file:
    process_metadata = yaml.safe_load(file)
    for process in process_metadata:
        metadata = Metadata(process)
        load_to_link(metadata)

with open("dwh/data_vault/metadata/lsat.yaml", "r") as file:
    process_metadata = yaml.safe_load(file)
    for process in process_metadata:
        metadata = Metadata(process)
        load_to_lsat(metadata)

with open("dwh/data_vault/metadata/ref.yaml", "r") as file:
    process_metadata = yaml.safe_load(file)
    for process in process_metadata:
        metadata = Metadata(process)
        load_to_ref(metadata)



