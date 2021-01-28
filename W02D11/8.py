def returnpalindrome(name):
    newstring = ""
    for i in range(len(name)-1,-1,-1):
        newstring=newstring+name[i]
    if newstring==name:
        return True
    else:
        return False

name=input("Enter input string:").lower()
torf=returnpalindrome(name)

if torf:
    print("The string {} is a palindrome".format(name))
else:
    print("The string {} is not a palindrome".format(name))




