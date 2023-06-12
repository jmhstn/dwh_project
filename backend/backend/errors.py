class FieldsNotFound(Exception):
    def __init__(self, fields: list[str]):
        message = "Could not find fields: " + ", ".join(fields) + "."
        super().__init__(message)
