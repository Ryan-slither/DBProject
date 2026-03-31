from datetime import date, timedelta

from mariadb import connect

from MARIADB_CREDS import DB_CONFIG
from models.Customer import Customer
from models.Item import Item
from models.Rental import Rental
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist

conn = connect(
    user=DB_CONFIG["username"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["host"],
    database=DB_CONFIG["database"],
    port=DB_CONFIG["port"],
)


cur = conn.cursor()


def add_item(new_item: Item | None = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    cur.execute("SELECT MAX(i_item_sk) FROM Item;")
    row = cur.fetchone()
    print(row)


def add_customer(new_customer: Customer | None = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    raise NotImplementedError("you must implement this function")


def edit_customer(
    original_customer_id: str | None = None, new_customer: Customer | None = None
):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    raise NotImplementedError("you must implement this function")


def rent_item(item_id: str | None = None, customer_id: str | None = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    raise NotImplementedError("you must implement this function")


def waitlist_customer(
    item_id: str | None = None, customer_id: str | None = None
) -> int:
    """
    Returns the customer's new place in line.
    """
    raise NotImplementedError("you must implement this function")


def update_waitlist(item_id: str | None = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    raise NotImplementedError("you must implement this function")


def return_item(item_id: str | None = None, customer_id: str | None = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    raise NotImplementedError("you must implement this function")


def grant_extension(item_id: str | None = None, customer_id: str | None = None):
    """
    Adds 14 days to the due_date.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_items(
    filter_attributes: Item | None = None,
    use_patterns: bool = False,
    min_price: float = -1,
    max_price: float = -1,
    min_start_year: int = -1,
    max_start_year: int = -1,
) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    query = """
            SELECT *
            FROM Item
            """
    string_symbol = "LIKE" if use_patterns else "="

    where_list = []
    where_values = []
    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            where_list.append(f"i_item_id {string_symbol} ?")
            where_values.append(filter_attributes.item_id)

        if filter_attributes.product_name is not None:
            where_list.append(f"i_product_name {string_symbol} ?")
            where_values.append(filter_attributes.product_name)

        if filter_attributes.brand is not None:
            where_list.append(f"i_brand {string_symbol} ?")
            where_values.append(filter_attributes.brand)

        if filter_attributes.category is not None:
            where_list.append(f"i_category {string_symbol} ?")
            where_values.append(filter_attributes.category)

        if filter_attributes.manufact is not None:
            where_list.append(f"i_manufact {string_symbol} ?")
            where_values.append(filter_attributes.manufact)

        if filter_attributes.current_price != -1:
            where_list.append("i_current_price = ?")
            where_values.append(filter_attributes.current_price)

        if filter_attributes.start_year != -1:
            where_list.append("YEAR(i_rec_start_date) = ?")
            where_values.append(filter_attributes.start_year)

        if filter_attributes.num_owned != -1:
            where_list.append("i_num_owned = ?")
            where_values.append(filter_attributes.num_owned)

    if min_price != -1:
        where_list.append("i_current_price >= ?")
        where_values.append(min_price)

    if max_price != -1:
        where_list.append("i_current_price <= ?")
        where_values.append(max_price)

    if min_start_year != -1:
        where_list.append("YEAR(i_rec_start_date) >= ?")
        where_values.append(min_start_year)

    if max_start_year != -1:
        where_list.append("YEAR(i_rec_start_date) <= ?")
        where_values.append(max_start_year)

    if len(where_list) > 0:
        query += "WHERE "
        for cond in where_list:
            query += cond
            if cond != where_list[-1]:
                query += "\n"
                query += " AND "

    query += ";"
    print(query)
    print()
    cur.execute(query, tuple(where_values))
    items = []
    for row in cur:
        items.append(
            Item(
                item_id=row[1],
                product_name=row[3],
                brand=row[4],
                category=row[6],
                manufact=row[7],
                current_price=row[8],
                start_year=row[2].year,
                num_owned=row[9],
            )
        )

    return items


def get_filtered_customers(
    filter_attributes: Customer | None = None, use_patterns: bool = False
) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rentals(
    filter_attributes: Rental | None = None,
    min_rental_date: str | None = None,
    max_rental_date: str | None = None,
    min_due_date: str | None = None,
    max_due_date: str | None = None,
) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(
    filter_attributes: RentalHistory | None = None,
    min_rental_date: str | None = None,
    max_rental_date: str | None = None,
    min_due_date: str | None = None,
    max_due_date: str | None = None,
    min_return_date: str | None = None,
    max_return_date: str | None = None,
) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(
    filter_attributes: Waitlist | None = None,
    min_place_in_line: int = -1,
    max_place_in_line: int = -1,
) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str | None = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    raise NotImplementedError("you must implement this function")


def place_in_line(item_id: str | None = None, customer_id: str | None = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    raise NotImplementedError("you must implement this function")


def line_length(item_id: str | None = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    raise NotImplementedError("you must implement this function")


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()
