from fdv import Fdv
from fd import Fd

v = Fdv()
fd = Fd("../dist/index.json")

fd.create_db()
table = fd.create_table("user", {
    "id": v.int().autoIncrement(),
    "name": v.str(),
    "email": v.email()
})

for i in range(30):
    table.insert({ "name": f"user {i + 1}", "email": f"user{i + 1}@gmail.com" })

print(table.select({ "id": {"st": "15"} }))