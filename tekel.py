

print("Welcome to Tekel")


serialNo = TekelFeature(TekelType.Digit)
transactionDate = TekelFeature(TekelType.Date)
refNumber = TekelFeature(TekelType.UniqueString)
valueDate = TekelFeature(TekelType.Date)
withdrawal = TekelFeature(TekelType.Currency)
deposit = TekelFeature(TekelType.Currency)
balance = TekelFeature(TekelType.Currency)

transactionList = TekelList()
