

print("Welcome to Tekel")


serialNo = TekelFeature(TekelType.Digit)
serialNo.examples("1", "2, "3")

transactionDate = TekelFeature(TekelType.Date)
transactionDate.examples("01/10/2018  12:00:00 AM", "10/2/2018  12:00:00 AM")

refNumber = TekelFeature(TekelType.UniqueString)
refNumber.examples("099MJNL182752NNC", "099MJNL182752Y0F")

valueDate = TekelFeature(TekelType.Date)
valueDate.examples(transactionDate)

withdrawal = TekelFeature(TekelType.Currency)
withdrawal.examples("1,830,438.30")

# TODO: indicate the relationship between the numbers so the AI knows how to find them
deposit = TekelFeature(TekelType.Currency)
balance = TekelFeature(TekelType.Currency)

transactionList = TekelList(serialNo, transactionDate, refNumber, valueDate, withdrawal, deposit, balance)

# Load the first bank statements
transactionList.loadFile("lolbank1.csv")

# load the second bank statements. Will avoid overlaps
transactionList.loadFile("lolbank2.csv", detectOverlaps=True)

# The task is to match each inflow in a statement to a corresponding sale, or a corresponding outflow in another of our
# bank accounts

salesDate = TekelFeature(TekelType.Date)
salesAmount = TekelFeature(TekelType.Currency)
salesItemName = TekelFeature(TekelType.String)

salesList = TekelList(salesDate, salesAmount, salesItemName)
salesList.LoadFile("salessheet.csv")

salesAmount.isEquivalentTo(deposit)
salesDate.isEquivalentTo(transactionDate)

salesList.matchWith(transactionList)