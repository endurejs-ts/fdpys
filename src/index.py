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

# force push
table.insert_many([
    {"name": "james", "email": "james39@gmail.com"},
    {"name": "james junior 2", "email": "james40@gmail.com"}
])

try:
    # no-force push
    table.insert({"name": "Alice", "email": "abcd1234gmail.com"})

except Exception as e:
    print(e)