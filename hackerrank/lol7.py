def numDecodings(s):
    a = 1
    b = 1

    if s[0] == '0': return 0

    for i in range(1, len(s)):
        temp = 0
        if s[i] >= '1' and s[i] <= '9':
            temp = b
        double = s[i - 1:i + 1]
        if double >= '10' and double <= '26':
            temp += a
        a = b
        b = temp
    return b


l = [
    # '12',
    # '226',
    # '226226',
    '777'
]

for s in l:
    print(s, numDecodings(s), sep=' = ')
    # res = [s[i: j] for i in range(len(s))  for j in range(i + 1, len(s) + 1) if len(s[i: j]) < 3 ]
    # print(res)



# 226226
# 2 26 22 6