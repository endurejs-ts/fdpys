from fdv import Fdv
from fd import Fd

v = Fdv()
fd = Fd("../dist/index.json")

fd.create_db()
table = fd.create_table("user", {
    "id": v.int().autoIncrement(),
    "name": v.str().unique(),
    "email": v.email()
})

table.insert({"name": "Alice", "email": "abcd1234gmail.com"})