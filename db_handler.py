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

    if new_item is None:
        raise ValueError("new_item cannot be None")

    query = """
            INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, i_brand, i_class, i_category, i_manufact, i_current_price, i_num_owned)
            VALUES (
                (SELECT COALESCE(MAX(i_item_sk), 0) + 1 FROM item AS tmp), 
                ?, ?, ?, ?, NULL, ?, ?, ?, ?
            );
            """

    cur.execute(
        query,
        (
            new_item.item_id,
            f"{new_item.start_year}-01-01",
            new_item.product_name,
            new_item.brand,
            new_item.category,
            new_item.manufact,
            new_item.current_price,
            new_item.num_owned,
        ),
    )


def add_customer(new_customer: Customer | None = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """

    if new_customer is None:
        raise ValueError("new_customer cannot be None")

    cur.execute(
        "SELECT COALESCE(MAX(ca_address_sk), 0) + 1 FROM customer_address AS tmp;"
    )
    addr_sk = cur.fetchone()[0]

    addr_query = """
                INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip)
                VALUES (?, ?, ?, ?, ?, ?);
                """

    street_info, city, state_info = map(
        lambda x: x.strip(), new_customer.address.split(",")
    )
    street_number, street_name = street_info.split(" ", maxsplit=1)
    state, zip = state_info.split(" ")

    cur.execute(addr_query, (addr_sk, street_number, street_name, city, state, zip))

    first_name, last_name = new_customer.name.split(" ")
    query = """
            INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk)
            VALUES ((SELECT COALESCE(MAX(c_customer_sk), 0) + 1 FROM customer AS tmp), ?, ?, ?, ?, ?);
            """

    cur.execute(
        query,
        (
            new_customer.customer_id,
            first_name,
            last_name,
            new_customer.email,
            addr_sk,
        ),
    )


def edit_customer(original_customer_id: str, new_customer: Customer | None = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    query = """
            UPDATE customer
            SET 
            """
    update_list = []
    update_values = []
    if new_customer is not None:
        if new_customer.customer_id is not None:
            update_list.append("c_customer_id = ?")
            update_values.append(new_customer.customer_id)

        if new_customer.email is not None:
            update_list.append("c_email_address = ?")
            update_values.append(new_customer.email)

        if new_customer.name is not None:
            first_name, last_name = new_customer.name.split(" ")
            update_list.append("c_first_name = ?, c_last_name = ?")
            update_values.append(first_name)
            update_values.append(last_name)

        if new_customer.address is not None:
            ca_query = """
                       SELECT c_current_addr_sk
                       FROM customer
                       WHERE c_customer_id = ?;
                       """
            cur.execute(ca_query, (original_customer_id,))
            addr_sk = cur.fetchone()[0]

            street_info, city, state_info = map(
                lambda x: x.strip(), new_customer.address.split(",")
            )
            street_number, street_name = street_info.split(" ", maxsplit=1)
            state, zip = state_info.split(" ")

            ca_sk_query = """
                         UPDATE customer_address
                         SET ca_street_number = ?, ca_street_name = ?, ca_city = ?, ca_state = ?, ca_zip = ?
                         WHERE ca_address_sk = ?;
                         """
            cur.execute(
                ca_sk_query, (street_number, street_name, city, state, zip, addr_sk)
            )

    if len(update_list) == 0:
        return

    for update in update_list:
        query += update
        if update != update_list[-1]:
            query += ", "

    query += ";"
    cur.execute(query, tuple(update_values))


def rent_item(item_id: str | None = None, customer_id: str | None = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    if item_id is None:
        raise ValueError("item_id cannot be None")

    if customer_id is None:
        raise ValueError("customer_id cannot be None")

    query = """
            INSERT INTO rental (item_id, customer_id, rental_date, due_date)
            VALUES (
                ?, ?, CURRENT_DATE(), DATE_ADD(CURRENT_DATE(), INTERVAL 14 DAY)
            );
            """

    cur.execute(
        query,
        (item_id, customer_id),
    )


def waitlist_customer(
    item_id: str | None = None, customer_id: str | None = None
) -> int:
    """
    Returns the customer's new place in line.
    """
    if item_id is None:
        raise ValueError("item_id cannot be None")

    if customer_id is None:
        raise ValueError("customer_id cannot be None")

    cur.execute("SELECT COUNT(*) FROM waitlist WHERE item_id = ?", (item_id,))
    last_line_place = cur.fetchone()[0] + 1

    query = """
            INSERT INTO waitlist (item_id, customer_id, place_in_line)
            VALUES (
                ?, ?, ?
            );
            """

    cur.execute(
        query,
        (item_id, customer_id, last_line_place),
    )

    return last_line_place


def update_waitlist(item_id: str | None = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    if item_id is None:
        raise ValueError("item_id cannot be None")

    cur.execute(
        """
            DELETE FROM waitlist
            WHERE item_id = ?
            AND place_in_line = 1;
        """,
        (item_id,),
    )

    cur.execute(
        """
            UPDATE waitlist
            SET place_in_line = place_in_line - 1
            WHERE item_id = ?;
        """,
        (item_id,),
    )


def return_item(item_id: str, customer_id: str):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    cur.execute(
        """
        SELECT *
        FROM rental
        WHERE item_id = ? AND customer_id = ?;
    """,
        (item_id, customer_id),
    )
    rental = cur.fetchone()

    cur.execute(
        """
        DELETE FROM rental
        WHERE item_id = ? AND customer_id = ?;
    """,
        (item_id, customer_id),
    )

    cur.execute(
        """
        INSERT INTO rental_history
        VALUES (?, ?, ?, ?, CURRENT_DATE());
    """,
        (rental[0], rental[1], rental[2], rental[3]),
    )


def grant_extension(item_id: str, customer_id: str):
    """
    Adds 14 days to the due_date.
    """
    cur.execute(
        """
        UPDATE rental
        SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY)
        WHERE item_id = ? AND customer_id = ?;
    """,
        (item_id, customer_id),
    )


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
    query = """
            SELECT *
            FROM customer
            INNER JOIN customer_address
            ON customer.c_current_addr_sk = customer_address.ca_address_sk
            """
    string_symbol = "LIKE" if use_patterns else "="

    where_list = []
    where_values = []
    if filter_attributes is not None:
        if filter_attributes.customer_id is not None:
            where_list.append(f"c_customer_id {string_symbol} ?")
            where_values.append(filter_attributes.customer_id)

        if filter_attributes.email is not None:
            where_list.append(f"c_email_address {string_symbol} ?")
            where_values.append(filter_attributes.email)

        if filter_attributes.name is not None:
            first_name, last_name = filter_attributes.name.split(" ")
            where_list.append(
                f"c_first_name {string_symbol} ? AND c_last_name {string_symbol} ?"
            )
            where_values.append(first_name)
            where_values.append(last_name)

        if filter_attributes.address is not None:
            street_info, city, state_info = map(
                lambda x: x.strip(), filter_attributes.address.split(",")
            )
            street_number, street_name = street_info.split(" ", maxsplit=1)
            state, zip = state_info.split(" ")

            ca_sk_query = f"""
                         SELECT ca_address_sk 
                         FROM customer_address
                         INNER JOIN customer
                         ON customer.c_current_addr_sk = customer_address.ca_address_sk
                         WHERE ca_street_number {string_symbol} ? AND
                               ca_street_name {string_symbol} ? AND
                               ca_city {string_symbol} ? AND
                               ca_state {string_symbol} ? AND
                               ca_zip {string_symbol} ?;
                         """
            cur.execute(ca_sk_query, (street_number, street_name, city, state, zip))
            ca_sk_results = cur.fetchall()
            ca_sk_list = [str(row[0]) for row in ca_sk_results]
            where_list.append(f"c_current_addr_sk IN ({','.join(ca_sk_list)})")

    if len(where_list) > 0:
        query += "WHERE "
        for cond in where_list:
            query += cond
            if cond != where_list[-1]:
                query += "\n"
                query += " AND "

    query += ";"
    cur.execute(query, tuple(where_values))
    customers = []
    for row in cur:
        customers.append(
            Customer(
                customer_id=row[1],
                email=row[4],
                name=row[2] + " " + row[3],
                address=row[7]
                + " "
                + row[8]
                + ", "
                + row[9]
                + ", "
                + row[10]
                + " "
                + row[11],
            )
        )

    return customers


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
    query = """
            SELECT *
            FROM rental
            """

    where_list = []
    where_values = []
    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            where_list.append("item_id = ?")
            where_values.append(filter_attributes.item_id)

        if filter_attributes.customer_id is not None:
            where_list.append("customer_id = ?")
            where_values.append(filter_attributes.customer_id)

        if filter_attributes.rental_date is not None:
            where_list.append("rental_date = ?")
            where_values.append(filter_attributes.rental_date)

        if filter_attributes.due_date is not None:
            where_list.append("due_date = ?")
            where_values.append(filter_attributes.due_date)

    if min_rental_date is not None:
        where_list.append("rental_date >= ?")
        where_values.append(min_rental_date)

    if max_rental_date is not None:
        where_list.append("rental_date <= ?")
        where_values.append(max_rental_date)

    if min_due_date is not None:
        where_list.append("due_date >= ?")
        where_values.append(min_due_date)

    if max_due_date is not None:
        where_list.append("due_date <= ?")
        where_values.append(max_due_date)

    if len(where_list) > 0:
        query += "WHERE "
        for cond in where_list:
            query += cond
            if cond != where_list[-1]:
                query += "\n"
                query += " AND "

    query += ";"
    print("get_filtered_rentals completed")
    print()
    cur.execute(query, tuple(where_values))
    rentals = []
    for row in cur:
        rentals.append(
            Rental(
                item_id=row[1], customer_id=row[2], rental_date=row[3], due_date=row[4]
            )
        )

    return rentals


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
    query = """
            SELECT *
            FROM rental_history
            """

    where_list = []
    where_values = []
    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            where_list.append("item_id = ?")
            where_values.append(filter_attributes.item_id)

        if filter_attributes.customer_id is not None:
            where_list.append("customer_id = ?")
            where_values.append(filter_attributes.customer_id)

        if filter_attributes.rental_date is not None:
            where_list.append("rental_date = ?")
            where_values.append(filter_attributes.rental_date)

        if filter_attributes.due_date is not None:
            where_list.append("due_date = ?")
            where_values.append(filter_attributes.due_date)

        if filter_attributes.return_date is not None:
            where_list.append("return_date = ?")
            where_values.append(filter_attributes.return_date)

    if min_rental_date is not None:
        where_list.append("rental_date >= ?")
        where_values.append(min_rental_date)

    if max_rental_date is not None:
        where_list.append("rental_date <= ?")
        where_values.append(max_rental_date)

    if min_due_date is not None:
        where_list.append("due_date >= ?")
        where_values.append(min_due_date)

    if max_due_date is not None:
        where_list.append("due_date <= ?")
        where_values.append(max_due_date)

    if min_return_date is not None:
        where_list.append("return_date >= ?")
        where_values.append(min_return_date)

    if max_return_date is not None:
        where_list.append("return_date <= ?")
        where_values.append(max_return_date)

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
    rentalHistories = []
    for row in cur:
        rentalHistories.append(
            RentalHistory(
                item_id=row[1],
                customer_id=row[2],
                rental_date=row[3],
                due_date=row[4],
                return_date=row[5],
            )
        )

    return rentalHistories


def get_filtered_waitlist(
    filter_attributes: Waitlist | None = None,
    min_place_in_line: int = -1,
    max_place_in_line: int = -1,
) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    query = """
            SELECT *
            FROM waitlist
            """

    where_list = []
    where_values = []
    if filter_attributes is not None:
        if filter_attributes.item_id is not None:
            where_list.append("item_id = ?")
            where_values.append(filter_attributes.item_id)

        if filter_attributes.customer_id is not None:
            where_list.append("customer_id = ?")
            where_values.append(filter_attributes.customer_id)

        if filter_attributes.place_in_line is not None:
            where_list.append("place_in_line = ?")
            where_values.append(filter_attributes.place_in_line)

    if min_place_in_line != -1:
        where_list.append("place_in_line >= ?")
        where_values.append(min_place_in_line)

    if max_place_in_line != -1:
        where_list.append("place_in_line <= ?")
        where_values.append(max_place_in_line)

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
    waitlists = []
    for row in cur:
        waitlists.append(
            Waitlist(item_id=row[1], customer_id=row[2], place_in_line=row[3])
        )

    return waitlists


def number_in_stock(item_id: str | None = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    if item_id is None:
        raise ValueError("item_id cannot be None")

    cur.execute(
        """ 
        SELECT i_num_owned
        FROM item
        WHERE i_item_id = ?;
        """,
        (item_id,),
    )
    possible_item = cur.fetchone()
    if len(possible_item) != 0:
        i_num_owned = possible_item[0]
    else:
        return -1

    cur.execute(
        """
        SELECT COUNT(*) 
        FROM rental 
        WHERE item_id = ?;
        """,
        (item_id,),
    )
    rentals_on_item = cur.fetchone()[0]

    return i_num_owned - rentals_on_item


def place_in_line(item_id: str, customer_id: str) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    cur.execute(
        """
        SELECT COALESCE(
            (
                SELECT place_in_line
                FROM waitlist
                WHERE item_id = ?
                    AND customer_id = ?
            ),
            -1
        ) AS res;
    """,
        (item_id, customer_id),
    )
    return cur.fetchone()[0]


def line_length(item_id: str | None = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    cur.execute(
        """
        SELECT COUNT(*)
        FROM waitlist
        WHERE item_id = ?;
    """,
        (item_id,),
    )
    return cur.fetchone()[0]


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
