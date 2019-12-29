
# averedge lenght of string


def lol1(s):
    ss = s.split()
    ln = len(ss)
    return sum(map(len, ss)) / ln


l = [
    'asd ds a'
    , 'Hi my name is Bob'
]

for i in l:
    print(i, lol1(i), sep=' = ')
