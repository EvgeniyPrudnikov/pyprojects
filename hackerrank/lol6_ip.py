def validIPAddress(IP: str) -> str:
    ln1 = len(IP.split('.'))  # possible ipv4
    ln2 = len(IP.split(':'))  # possible ipv6

    if ln1 == 4:
        sip = IP.split('.')
        try:
            dg = all(map(lambda x: 0 <= int(x) <= 255 and len(x) <= 3 and str(int(x)) == x, sip))
            if not dg:
                return 'Neither'
            else:
                return 'IPv4'
        except:
            return 'Neither'

    elif ln2 == 8:
        sip = IP.split(':')
        try:
            dg = all(map(lambda x: 0 <= int(x, 16) <= 65535 and len(x) <= 4 and x[0].upper() in list('0123456789ABCDEF'), sip))
            if not dg:
                return 'Neither'
            else:
                return 'IPv6'
        except:
            return 'Neither'

    else:
        return 'Neither'


l = [ '1asd72.16.254.125'
, '01.01.01.01'
, '192.0.0.1'
,'172.16.254.1'
,'2001:0db8:85a3:0:-0:8A2E:0370:7334'
,'256.256.256.256'
,'200-1:0db8:85a3:0:0:8A2E:0370:7334']


for i in l:
    print(i,validIPAddress(i),sep=' = ')