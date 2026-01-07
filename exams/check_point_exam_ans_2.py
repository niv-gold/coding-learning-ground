
def solution(D, X):
    # Implement your solution here
    Lp=0
    Rp=0
    cnt=1
    while Rp < len(D):
        Rp+=1
        win_min = min(D[Lp:Rp+1])
        win_max = max(D[Lp:Rp+1])
        if (win_max-win_min) > X:
            Lp=Rp
            cnt+=1
    return cnt

D = [2,5,9,2,1,4]
X=3
solution(D,X)