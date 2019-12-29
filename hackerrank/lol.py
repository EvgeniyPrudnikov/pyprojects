

# merge two sorted lists in O(n)

l1 = [1, 2, 3, 5, 6, 7]
l2 = [1, 3, 4, 4, 4, 4, 4]


def merge1(l1, l2):
    lres = []
    i = j = 0
    while i < len(l1) or j < len(l2):

        if j >= len(l2):
            lres.append(l1[i])
            i += 1
        elif i >= len(l1):
            lres.append(l2[j])
            j += 1
        elif l1[i] >= l2[j]:
            lres.append(l2[j])
            j += 1
        else:
            lres.append(l1[i])
            i += 1
    return lres


l3 = merge1(l1, l2)

print(l3)
