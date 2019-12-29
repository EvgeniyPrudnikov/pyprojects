# https://www.hackerrank.com/challenges/piling-up/problem


def check(num, k):
    i = 0
    j = num - 1
    tmp = max(k[i], k[j])
    for n in range(num):
        if i == j and k[i] <= tmp:
            continue
        elif k[i] >= k[j] and k[i] <= tmp:
            tmp = k[i]
            i += 1
            continue
        elif k[i] < k[j] and k[j] <= tmp:
            tmp = k[j]
            j -= 1
            continue
        else:
            print('No')
            return
    print('Yes')


def main():
    cube_num = 0
    cube_lengths = []

    s = input()
    for i in range(int(s)):
        cube_num = int(input())
        cube_lengths = list(map(int, list(input().split(' '))))
        check(cube_num, cube_lengths)


if __name__ == '__main__':
    main()
