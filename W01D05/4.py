'''This program is used to print the length of a string'''

name=input("Enter your name:")
print("Length of your name is:",len(name)) #Printing length of entered string

'''This program is used to check if a string is palindrome or not'''

enteredstring=(input("Enter your text:")).lower()
reversedstring=""
length=len(enteredstring)
for i in reversed(enteredstring): #iterating string in reverse order
    reversedstring=reversedstring+i
if enteredstring==reversedstring:
    print("Your string {} is a palindrome string".format(enteredstring))
else:
    print("Your string {} is not a palindrome string".format(enteredstring))

'''This program is used to check if today's closing price in nifty is higher
 than last 52 week high or not'''

todaycp=float(input("Enter today's closing price of Nifty:"))
highprice52wk=float(input("Enter last 52 week high price:"))
if todaycp>highprice52wk: #Checking condition if today's close is higher than 52 week high
    print("Today's closing price higher than last 52 week high price")
else:
    print("Today's closing price is lower than last 52 week high price")
