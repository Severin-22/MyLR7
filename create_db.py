import mysql.connector
from mysql.connector import Error

def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='app_user',
            password='user_password456',
            database='my_database'
        )
        print("З'єднання з базою даних успішно встановлено")
    except Error as e:
        print(f"Виникла помилка: {e}")

    return connection

def create_tables(connection):
    cursor = connection.cursor()

    # Видалення таблиць, якщо вони існують
    cursor.execute("DROP TABLE IF EXISTS Sales;")
    cursor.execute("DROP TABLE IF EXISTS Products;")
    cursor.execute("DROP TABLE IF EXISTS Clients;")

    # Створення таблиць
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Clients (
        client_id INT AUTO_INCREMENT PRIMARY KEY,
        company_name VARCHAR(255) NOT NULL,
        client_type ENUM('Юридична', 'Фізична') NOT NULL,
        address VARCHAR(255),
        phone VARCHAR(50),
        contact_person VARCHAR(255),
        account_number VARCHAR(50)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Products (
        product_id INT AUTO_INCREMENT PRIMARY KEY,
        product_name VARCHAR(255) NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        quantity INT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Sales (
        sale_id INT AUTO_INCREMENT PRIMARY KEY,
        sale_date DATE NOT NULL,
        client_id INT,
        product_id INT,
        quantity_sold INT NOT NULL CHECK (quantity_sold > 0),
        discount DECIMAL(5, 2) CHECK (discount >= 3 AND discount <= 20),
        payment_method ENUM('Готівковий', 'Безготівковий') NOT NULL,
        delivery_required BOOLEAN,
        delivery_cost DECIMAL(10, 2),
        FOREIGN KEY (client_id) REFERENCES Clients(client_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES Products(product_id) ON DELETE CASCADE
    );
    """)

    connection.commit()
    cursor.close()
    print("Таблиці створені успішно")

def insert_data(connection):
    cursor = connection.cursor()

    clients = [
        ("Фірма А", "Юридична", "вул. 1, Київ", "+380123456789", "Іванов Іван", "UA12345678901234567890"),
        ("Фірма Б", "Юридична", "вул. 2, Київ", "+380987654321", "Петренко Петро", "UA98765432101234567890"),
        ("ПП Клієнт", "Фізична", "вул. 3, Київ", "+380555555555", "Сидоренко Сидір", "UA13579246801234567890"),
        ("Клієнт Фізична", "Фізична", "вул. 4, Київ", "+380666666666", "Шевченко Тарас", "UA24681357901234567890"),
    ]

    cursor.executemany("""
        INSERT INTO Clients (company_name, client_type, address, phone, contact_person, account_number)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, clients)

    products = [
        ("Товар 1", 100.00, 50),
        ("Товар 2", 150.50, 30),
        ("Товар 3", 200.75, 20),
        ("Товар 4", 50.25, 15),
        ("Товар 5", 300.99, 5),
        ("Товар 6", 450.00, 10),
        ("Товар 7", 75.00, 40),
        ("Товар 8", 120.00, 25),
        ("Товар 9", 60.00, 35),
        ("Товар 10", 90.00, 45),
    ]

    cursor.executemany("""
        INSERT INTO Products (product_name, price, quantity)
        VALUES (%s, %s, %s);
    """, products)

    sales = [
        ("2024-10-01", 1, 1, 10, 5.00, "Готівковий", True, 20.00),
        ("2024-10-02", 2, 2, 5, 10.00, "Безготівковий", False, 0.00), 
        ("2024-10-03", 3, 3, 3, 3.00, "Готівковий", True, 15.00),  
        ("2024-10-04", 4, 4, 1, 18.00, "Безготівковий", True, 5.00),  
        ("2024-10-05", 1, 5, 2, 10.00, "Готівковий", False, 10.00),  
        ("2024-10-06", 2, 6, 7, 15.00, "Безготівковий", True, 12.00),  
        ("2024-10-07", 3, 7, 4, 7.00, "Готівковий", False, 5.00),  
        ("2024-10-08", 4, 8, 6, 20.00, "Готівковий", True, 10.00),  
        ("2024-10-09", 1, 9, 3, 11.00, "Безготівковий", True, 15.00),  
        ("2024-10-10", 2, 10, 2, 3.00, "Готівковий", False, 0.00),  
        ("2024-10-11", 1, 1, 1, 4.00, "Готівковий", True, 5.00),  
        ("2024-10-12", 2, 2, 2, 6.00, "Безготівковий", False, 0.00),  
        ("2024-10-13", 3, 3, 1, 8.00, "Готівковий", True, 10.00),  
        ("2024-10-14", 4, 4, 3, 12.00, "Безготівковий", True, 20.00),  
        ("2024-10-15", 1, 5, 2, 9.00, "Готівковий", True, 7.00),  
        ("2024-10-16", 2, 6, 4, 14.00, "Безготівковий", False, 5.00), 
        ("2024-10-17", 3, 7, 3, 13.00, "Готівковий", True, 8.00),  
        ("2024-10-18", 4, 8, 1, 15.00, "Безготівковий", False, 0.00),  
        ("2024-10-19", 1, 9, 2, 4.00, "Готівковий", True, 6.00),  
    ]

    cursor.executemany("""
        INSERT INTO Sales (sale_date, client_id, product_id, quantity_sold, discount, payment_method, delivery_required, delivery_cost)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, sales)

    connection.commit()
    cursor.close()
    print("Таблиці заповнені даними успішно")

def main():
    connection = create_connection()
    if connection:
        create_tables(connection)
        insert_data(connection)
        connection.close()

if __name__ == "__main__":
    main()
