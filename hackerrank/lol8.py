
# somethin with friends


def lol(g):
    d = dict()

    for x in g:
        if len(x) >= 3:
            continue

        if len(x) == 1 and x[0] not in d.keys():
            d[x[0]] = 0
        elif len(x) == 2 and x[0] not in d.keys() and x[0] != x[1]:
            d[x[0]] = 1
        elif len(x) == 2 and x[0] in d.keys() and x[0] != x[1]:
            d[x[0]] += 1

    return d


l = [[['A','B'],['A','C'],['A','A'],['B','D'],['B','C'],['R','M'], ['S','S'],['P'], ['A']]]

for i in l:
    print(i, lol(i), sep=' = ')
