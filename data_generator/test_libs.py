from libs.generator import FakeAccountGenerator, FakeCustomerGenerator, FakeTransactionGenerator

account = FakeAccountGenerator()

list = account.create_data(1000)

print(list[0])
print(list[1].head())
account.to_file()
print(account.checkBankCustomerInitStatus())

account.stop_spark()


customer = FakeCustomerGenerator()

list = customer.create_data(1000)

print(list[0])
print(list[1].head())
customer.to_file()

customer.stop_spark()

transaction = FakeTransactionGenerator()

list = transaction.create_data(1000)

print(list[0])
print(list[1].head())
transaction.to_file()
print(transaction.checkBankAccountInitStatus())

transaction.stop_spark()

