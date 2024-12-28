import json
# from uuid import uuid4
from typing import Dict, Any, List
from fdv import FdvOptions

# Fd 클래스 정의를 먼저 합니다.
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
            self.database = {"type": "database", "tables": {}}
            self.saveInternal()

    def saveInternal(self):
        try:
            with open(self.filepath, 'w') as f:
                json.dump(self.database, f, indent=4)
        except FileNotFoundError:
            return {"type": "error", "value": {}, "msg": "fileNotFound"}

    def create_db(self):
        self.database = {"type": "database", "tables": {}}
        self.saveInternal()
    
    def drop_table(self, name: str):
        """Deletes a table from the database."""
        if name not in self.database["tables"]:
            raise ValueError(f"Table '{name}' does not exist.")
        del self.database["tables"][name]
        self.saveInternal()

    def create_table(self, name: str, schema: Dict[str, FdvOptions]) -> 'Table':
        if name in self.database["tables"]:
            raise ValueError(f"Table '{name}' already exists.")
        
        # Convert FdvOptions to dict before saving
        schema_dict = {col: options.to_dict() if isinstance(options, FdvOptions) else options
                    for col, options in schema.items()}
        
        self.database["tables"][name] = {
            "type": "table",
            "columns": schema_dict,
            "data": [],
            "current_id": 0
        }
        
        return Table(name, schema_dict, self)  # Fd 인스턴스를 Table에 전달
    
    def truncate_table(self, table_name: str):
        if table_name not in self.database["tables"]:
            raise ValueError(f"Table '{table_name}' does not exist.")
        
        self.database["tables"][table_name]["data"] = []
        self.saveInternal()

    def begin_transaction(self) -> 'Transaction':
        """트랜잭션 객체 반환"""
        return Transaction(self)

# Table 클래스 정의 (Fd 인스턴스를 db 매개변수로 사용)
class Table:
    def __init__(self, name, schema, db: Fd):
        self.name = name
        self.schema = schema
        self.data = []
        self.db = db  # Fd 인스턴스를 참조

    def insert(self, row: Dict[Any, Any]):
        # 데이터 삽입 시 autoIncrement 처리
        for col, value in self.schema.items():
            if "autoincrement" in value.get("options", []) and col not in row:
                row[col] = self.get_next_autoincrement_value_internal(col)
        self.data.append(row)
        # 변경된 데이터를 Fd 데이터베이스에 반영
        self.db.database["tables"][self.name]["data"] = self.data
        self.db.saveInternal()  # 데이터베이스 저장

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
        # 변경된 데이터를 Fd 데이터베이스에 반영
        self.db.database["tables"][self.name]["data"] = self.data
        self.db.saveInternal()

    def delete(self, condition):
        self.data = [row for row in self.data if not self.match_condition(row, condition)]
        # 변경된 데이터를 Fd 데이터베이스에 반영
        self.db.database["tables"][self.name]["data"] = self.data
        self.db.saveInternal()

    def match_condition(self, row, condition):
        # AND 조건: condition이 리스트인 경우 모든 조건을 만족해야 True
        if isinstance(condition, list):
            for sub_condition in condition:
                if not self.match_condition(row, sub_condition):
                    return False
            return True

        # 기존 조건 처리
        for key, value in condition.items():
            if isinstance(value, dict):
                for operator, val in value.items():
                    if operator == "gt" and not row[key] > int(val):
                        return False
                    elif operator == "st" and not row[key] < int(val):
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
    
    def truncate(self):
        self.data = []
        self.db.database["tables"][self.name]["data"] = self.data
        self.db.saveInternal()

    def select(self, condition: Dict[Any, Any] = None):
        if condition:
            return [row for row in self.data if self.match_condition(row, condition)]
        return self.data

    def show(self):
        return self.data
    
    def join(self, other_table, on_condition, type = "inner"):
        result = []

        if type == "inner":
            for row1 in self.data:
                for row2 in other_table.data:
                    if on_condition(row1, row2):
                        result.append({**row1, **row2})

        elif type.lower() == "left":
            for row1 in self.data:
                matched = False
                for row2 in other_table.data:
                    if on_condition(row1, row2):
                        result.append({**row1, **row2})
                        matched = True
                if not matched:
                    result.append({**row1, **{col: None for col in other_table.schema.keys()}})

        elif type.lower() == "right":     
            for row2 in other_table.data:
                matched = False
                for row1 in self.data:
                    if on_condition(row1, row2):
                        result.append({**row1, **row2})
                        matched = True
                if not matched:
                    result.append({**{col: None for col in self.schema.keys()}, **row2})

        elif type.lower() == "full":
            seen = set()
            for row1 in self.data:
                matched = False
                for row2 in other_table.data:
                    if on_condition(row1, row2):
                        result.append({**row1, **row2})
                        matched = True
                        seen.add(id(row2))
                
                if not matched:
                    result.append({**row1, **{col: None for col in other_table.schema.keys()}})

            for row2 in other_table.data:
                if id(row2) not in seen:
                    result.append({**{col: None for col in self.schema.keys()}, **row2})
            
        return result
    
class Transaction(Fd):
    def __init__(self):
        self.fp = super().filepath
        self.database = super().load_db()
        self.auto = super().auto
        self.transaction = None

    def begin(self):
        if self.transaction is not None:
            raise ValueError("Transaction already in progress.")
        self.transaction = json.loads(json.dumps(self.database))

    def commit(self):
        if self.transaction is None:
            raise ValueError("No transaction in progress.")
        self.database = self.transaction
        self.transaction = None
        self.RsaveInternl()

    def rollback(self):
        if self.transaction is None:
            raise RuntimeError("No transaction in progress.")
        self.database = self.transaction
        self.transaction = None
        self.RsaveInternl()

    def RsaveInternl(self):
        if self.transaction is not None:
            return
        
        with open(self.fp, 'w') as f:
            json.dump(self.database, f, indent=4)
    
    def create_transaction(self, name, schema):
        if self.transaction is not None:
            db_ref = self.transaction

        else:
            db_ref = self.database
        
        if name in db_ref["tables"]:
            raise ValueError(f"Table '{name}' already exists.")
        
        db_ref["tables"][name] = {"types": "table", "columns": schema, "data": [], "current_id": 0}