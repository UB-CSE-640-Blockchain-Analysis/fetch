l = [{"value": "1", "addr": "123"}, {"value": "2", "addr": "789"}]
for el in l:
    if el["addr"] == "789":
        el["value"] = int(el["value"]) + 11
        print(el)
print(l.index({"value": "2", "addr": "789"}))
