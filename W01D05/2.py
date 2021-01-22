enteredstring=(input("Enter your text:")).lower()
reversedstring=""
length=len(enteredstring)
for i in reversed(enteredstring):
    reversedstring=reversedstring+i
if enteredstring==reversedstring:
    print("Your string {} is a palindrome string".format(enteredstring))
else:
    print("Your string {} is not a palindrome string".format(enteredstring))