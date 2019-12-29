# https://www.hackerrank.com/challenges/most-commons/submissions/code/135888832


d = {}

if __name__ == '__main__':
    s = input()
    for i in s:
        if i in d:
            d[i] += 1
        else:
            d[i] = 1

    res = sorted(sorted([(k, v) for k, v in d.items()], key=lambda x: x[0]), key=lambda x: x[1], reverse=True)[:3]
    for i in res:
        print(*i, sep=' ')
