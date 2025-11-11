# Question 1a
# Create a class BankAccount with private attribute _balance
class BankAccount:

    def __init__(self, initial_balance=0):
        self._balance = initial_balance

    def deposit(self, amount):
        if amount > 0:
            self._balance += amount
            return f" Deposited Shs{amount}"
        return " Invalid deposit amount."

    def withdraw(self, amount):
        if amount > 0:
            if self._balance >= amount:
                self._balance -= amount
                return f" Withdrew ${amount}"
            return " Insufficient funds."
        return " Invalid withdrawal amount."

    def get_balance(self):
        return self._balance


def bank_account_interaction():
    print("\ BANKING PROCESS")
    initial_balance = float(input("Enter initial balance for your account: "))
    account = BankAccount(initial_balance)

    while True:
        print("\n Choose an action:")
        print("1. Deposit")
        print("2. Withdraw")
        print("3. Check Balance")
        print("4. Exit")

        choice = input("Enter choice (1-4): ")

        if choice == '1':
            amount = float(input("Enter amount to deposit: "))
            print(account.deposit(amount))

        elif choice == '2':
            amount = float(input("Enter amount to withdraw: "))
            print(account.withdraw(amount))

        elif choice == '3':
            print(f"Current Balance: ${account.get_balance():.2f}")

        elif choice == '4':
            print("Exiting")
            break

        else:
            print("Invalid choice. Please try again.")


#1b solutions
class Student:
    def __init__(self, name, mark):
        self.name = name
        self.mark = mark

    def display_details(self):
        return f"{self.name} => {self.mark}"


def calculate_average_marks(students):
    if not students:
        return 0
    total = sum(student.mark for student in students)
    return total / len(students)


def student_record_interaction():
    print("\n STUDENT RECORD MANAGEMENT SYSTEM")
    students = []

    n = int(input("Enter number of students: "))
    for i in range(n):
        name = input(f"Enter name of student {i + 1}: ")
        mark = float(input(f"Enter mark of {name}: "))
        students.append(Student(name, mark))

    print("\n Details Of Student")
    for student in students:
        print(student.display_details())

    average = calculate_average_marks(students)
    print(f"\n Average Mark: {average:.2f}")


# the main function call
if __name__ == "__main__":
    print("QUESTION TOGGLE OPTIONS OF PART A AND B")
    print("1. Use BankAccount Class")
    print("2. Use Student Record Management")
    print("3. Run Both")
    print("4. Exit")

    while True:
        option = input("\nEnter your choice (1-4): ")

        if option == '1':
            bank_account_interaction()
        elif option == '2':
            student_record_interaction()
        elif option == '3':
            bank_account_interaction()
            student_record_interaction()
        elif option == '4':
            print("Program Ended.")
            break
        else:
            print("Invalid option. Please choose between 1 and 4.")
