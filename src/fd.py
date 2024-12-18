import json
from uuid import uuid4
from typing import Dict, Any, List

class Table:
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self.data = []

    def insert(self, row: Dict[Any, Any]):
        # 데이터 삽입 시 autoIncrement 처리
        for col, value in self.schema.items():
            if "autoincrement" in value.get("options", []) and col not in row:
                row[col] = self.get_next_autoincrement_value_internal(col)
        self.data.append(row)

    def insert_many(self, rows: List):
        for row in rows:
            self.insert(row)

    def get_next_autoincrement_value_internal(self, column):
        last_value = max([row[column] for row in self.data], default=0)
        return last_value + 1

    def modify(self, condition, updates):
        for row in self.data:
            if self.match_condition(row, condition):
                for key, value in updates.items():
                    if key in row:
                        row[key] = value

    def delete(self, condition):
        self.data = [row for row in self.data if not self.match_condition(row, condition)]

    def match_condition(self, row, condition):
        for key, value in condition.items():
            if isinstance(value, dict):
                for operator, val in value.items():
                    if operator == "gt" and not row[key] > val:
                        return False
                    elif operator == "lt" and not row[key] < val:
                        return False
                    elif operator == "eq" and not row[key] == val:
                        return False
                    elif operator == "ne" and not row[key] != val:
                        return False
                    elif operator == "in" and row[key] not in val:
                        return False
                    elif operator == "like" and val not in row[key]:
                        return False
            else:
                if row[key] != value:
                    return False
        return True

    def select(self, condition: Dict[Any, Any] = None):
        if condition:
            return [row for row in self.data if self.match_condition(row, condition)]
        return self.data

    def show(self):
        return self.data

class Fd:
    def __init__(self, filepath, autosave=True):
        self.filepath = filepath
        self.database = self.load_db()
        self.auto = autosave

    def load_db(self):
        try:
            with open(self.filepath, 'r') as f:
                content = f.read().strip()

                if content:
                    self.database = json.loads(content)
                
                else:
                    self.database = {"type": "database", "tables": {}}
                    self.saveInternal()
                    
        except FileNotFoundError:
            return {"type": "database", "tables": {}}
        
        except json.JSONDecodeError:
            # 파일이 있지만 JSON 형식이 올바르지 않으면 기본 구조로 초기화
            self.databases = {"type": "database", "tables": {}}
            self.saveInternal()

    def saveInternal(self):
        with open(self.filepath, 'w') as f:
            json.dump(self.database, f, indent=4)

    def create_db(self):
        self.database = {"type": "database", "tables": {}}
        self.saveInternal()

    def create_table(self, name: str, schema: Dict[str, Any]) -> Table:
        if name in self.database["tables"]:
            raise ValueError(f"Table '{name}' already exists.")
        self.database["tables"][name] = {
            "type": "table",
            "columns": schema,
            "data": [],
            "current_id": 0
        }
        self.saveInternal()

    def get_table(self, name):
        if name not in self.database["tables"]:
            raise ValueError(f"Table '{name}' does not exist.")
        return Table(self.database["tables"][name], self)