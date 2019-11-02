l = [['Harry', 37.21], ['Berry', 37.21], ['Tina', 37.2], ['Akriti', 41.0], ['Harsh', 39.0]]

print(l)

mn = min(map(lambda x: x[1], l))
mx = max(map(lambda x: x[1], l))

names = []

if mn == mx:
    for i in sorted(l, key=lambda x: x[0]):
        print(i[0])
else:

    prev = mx
    for i in sorted(l, key=lambda x: x[1]):
        v = i[1]
        if v > prev:
            break
        elif v > mn:
            names.append(i[0])
            prev = v
        else:
            continue

for name in sorted(names):
    print(name)
