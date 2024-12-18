import re

class FdvOptions:
    def __init__(self, col_type):
        self.definition = {"type": col_type, "options": []}

    def autoIncrement(self):
        if "autoincrement" in self.definition["options"]:
            raise ValueError("autoIncrement can only be applied once.")
        self.definition["options"].append("autoincrement")
        return self.definition  # 바로 최종 결과 반환

    def primary(self):
        if "primary" in self.definition["options"]:
            raise ValueError("primary can only be applied once.")
        self.definition["options"].append("primary")
        return self.definition

    def unique(self):
        if "unique" in self.definition["options"]:
            raise ValueError("unique can only be applied once.")
        self.definition["options"].append("unique")
        return self.definition

    def validate(self, validator):
        self.definition["validate"] = validator
        return self.definition


class Fdv:
    def int(self) -> FdvOptions:
        return FdvOptions("int")

    def str(self) -> FdvOptions:
        return FdvOptions("str")

    def email(self) -> FdvOptions:
        return FdvOptions("str").validate(
            lambda value: bool(re.match(r"[^@]+@[^@]+\.[^@]+", value))
        )
