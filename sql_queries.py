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

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall(), [i[0] for i in cursor.description]  # отримання заголовків стовпців
    except Error as e:
        print(f"Виникла помилка: {e}")
    finally:
        cursor.close()

def main():
    connection = create_connection()
    if connection:
        # Вивід даних з таблиці Клієнти
        print("Дані з таблиці Клієнти:")
        clients_query = "SELECT * FROM Clients;"
        clients_data, clients_headers = execute_query(connection, clients_query)
        print(clients_headers)
        for row in clients_data:
            print(row)

         # Вивід даних з таблиці Продажі
        print("\nДані з таблиці Продажі:")
        sales_query = "SELECT * FROM Sales;"
        sales_data, sales_headers = execute_query(connection, sales_query)
        print(sales_headers)
        for row in sales_data:
            print(row)

        # Вивід даних з таблиці Продукти
        print("\nДані з таблиці Продукти:")
        products_query = "SELECT * FROM Products;"
        products_data, products_headers = execute_query(connection, products_query)
        print(products_headers)
        for row in products_data:
            print(row)

        # Відобразити всі продажі, які були оплачені готівкою
        print("\nПродажі, оплачені готівкою:")
        cash_sales_query = """
        SELECT s.sale_id, c.company_name, s.sale_date, s.quantity_sold, s.discount
        FROM Sales s
        JOIN Clients c ON s.client_id = c.client_id
        WHERE s.payment_method = 'Готівковий'
        ORDER BY c.company_name;
        """
        cash_sales_data, cash_sales_headers = execute_query(connection, cash_sales_query)
        print(cash_sales_headers)
        for row in cash_sales_data:
            print(row)

        # Відобразити всі продажі, по яких потрібна була доставка
        print("\nПродажі, для яких потрібна була доставка:")
        delivery_sales_query = """
        SELECT s.sale_id, c.company_name, s.sale_date, s.quantity_sold, s.discount
        FROM Sales s
        JOIN Clients c ON s.client_id = c.client_id
        WHERE s.delivery_required = TRUE;
        """
        delivery_sales_data, delivery_sales_headers = execute_query(connection, delivery_sales_query)
        print(delivery_sales_headers)
        for row in delivery_sales_data:
            print(row)

        # Порахувати суму та суму з урахуванням скидки для кожного клієнта
        print("\nСума та сума з урахуванням скидки для кожного клієнта:")
        total_discount_query = """
        SELECT c.company_name, SUM(s.quantity_sold * p.price) AS total_amount,
               SUM(s.quantity_sold * p.price * (1 - s.discount / 100)) AS total_with_discount
        FROM Sales s
        JOIN Clients c ON s.client_id = c.client_id
        JOIN Products p ON s.product_id = p.product_id
        GROUP BY c.company_name;
        """
        total_discount_data, total_discount_headers = execute_query(connection, total_discount_query)
        print(total_discount_headers)
        for row in total_discount_data:
            print(row)

        # Відобразити всі покупки вказаного клієнта
        client_id = 1  # Приклад: замініть 1 на бажаний client_id
        print(f"\nПокупки клієнта з ID {client_id}:")
        client_purchases_query = f"""
        SELECT s.sale_id, s.sale_date, p.product_name, s.quantity_sold
        FROM Sales s
        JOIN Products p ON s.product_id = p.product_id
        WHERE s.client_id = {client_id};
        """
        client_purchases_data, client_purchases_headers = execute_query(connection, client_purchases_query)
        print(client_purchases_headers)
        for row in client_purchases_data:
            print(row)

        # Порахувати кількість покупок, які скоїв кожен клієнт
        print("\nКількість покупок, які зробив кожен клієнт:")
        purchase_count_query = """
        SELECT c.company_name, COUNT(s.sale_id) AS purchase_count
        FROM Sales s
        JOIN Clients c ON s.client_id = c.client_id
        GROUP BY c.company_name;
        """
        purchase_count_data, purchase_count_headers = execute_query(connection, purchase_count_query)
        print(purchase_count_headers)
        for row in purchase_count_data:
            print(row)

        # Порахувати суму, яку сплатив кожен клієнт за готівковим та безготівковим розрахунком
        print("\nСума, яку сплатив кожен клієнт за готівковим та безготівковим розрахунком:")
        payment_method_query = """
        SELECT c.company_name, s.payment_method, SUM(s.quantity_sold * p.price) AS total_paid
        FROM Sales s
        JOIN Clients c ON s.client_id = c.client_id
        JOIN Products p ON s.product_id = p.product_id
        GROUP BY c.company_name, s.payment_method;
        """
        payment_method_data, payment_method_headers = execute_query(connection, payment_method_query)
        print(payment_method_headers)
        for row in payment_method_data:
            print(row)

        connection.close()

if __name__ == "__main__":
    main()
