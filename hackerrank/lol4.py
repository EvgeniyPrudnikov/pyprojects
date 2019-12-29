# https://www.hackerrank.com/challenges/kangaroo/problem

def kangaroo(x1, v1, x2, v2):
    if x1 == x2 and v1 != v2:
        return 'NO'
    elif x1!= x2 and v1 == v2:
        return 'NO'
    else:
        N = (x1 - x2)/(v2 - v1)
        print(N)
        print(N.is_integer())
        if N > 0 and N.is_integer():
            return 'YES'
        else:
            return 'NO'


res = kangaroo(21, 6, 47, 3)
print(res)