import mysql.connector
from numpy.random import randint, choice
from random import randrange
from datetime import timedelta, datetime

def reset_database(tunnel, hostname, username, password, database):
    """ drop the database and create all webshop-related tables (DDL, no objects!) """
    print(f"Resetting database {database}...")
    
    with tunnel:
        conn = mysql.connector.connect(host=hostname, user=username, passwd=password, autocommit=True, database=database, auth_plugin='mysql_native_password')
        cur = conn.cursor()
        print("... dropping database")
        # first drop database to clear all tables and afterwards create new database webshop
        cur = conn.cursor()
        cur.execute (f"drop database if exists {database}")
        cur.execute (f"create database {database}")

        conn.close()

        # now execute all DDL statements
        conn = mysql.connector.connect(host=hostname, user=username, passwd=password, autocommit=True, database=database, auth_plugin='mysql_native_password')
        print("... creating tables")

        print("... ... status")
        cur = conn.cursor()
        cur.execute("""create table status (
        statusid int primary key,
        bearbeitungsstatus varchar(50)
        );""")

        print("... ... kategorie")
        cur.execute("""create table kategorie (
        katid int primary key,
        bezeichnung varchar(50)
        );""")

        print("... ... benutzer")
        cur.execute("""create table benutzer (
        email varchar(100) primary key,
        passwort varchar(50) not null,
        vorname varchar(50) not null,
        nachname varchar(50) not null
        );""")

        print("... ... produkt")
        cur.execute("""create table produkt (
        pnr int primary key,
        bezeichnung varchar(50),
        angelegt_am timestamp,
        verfuegbarkeit int not null,
        preis decimal(6,2) not null,
        katid int,
        foreign key (katid) references kategorie(katid) on delete cascade
        );""")

        print("... ... abteilung")
        cur.execute("""create table abteilung (
        abtid int primary key,
        bezeichnung varchar(50),
        uebergeordnet_abtid int, -- Fremdschlüssel hier nicht NOT NULL! Kardinalitäten der Hierarchie!
        foreign key (uebergeordnet_abtid) references abteilung (abtid) on delete cascade  -- Fremdschlüssel-Constraint
        );""")


        print("... ... kunde")
        cur.execute("""create table kunde (
        email varchar(100) primary key, -- Primärschlüssel und gleichzeitg Fremdschlüssel
        kdnr int unique,  -- Spalte soll eindeutig sein
        strasse varchar(50),
        hausnummer varchar(10),
        plz varchar(5),
        ort varchar(50),
        geburtsdatum date,
        foreign key (email) references benutzer (email) on delete cascade  -- Fremdschlüssel-Constraint
        );""")

        print("... ... mitarbeiter")
        cur.execute("""create table mitarbeiter (
        email varchar(100) primary key, -- Primärschlüssel und gleichzeitg Fremdschlüssel
        manr int unique,  -- Spalte soll eindeutig sein
        telefonnr varchar(30),
        abtid int not null,
        foreign key (email) references benutzer (email) on delete cascade,  -- Fremdschlüssel-Constraint email
        foreign key (abtid) references abteilung (abtid) on delete cascade  -- Fremdschlüssel-Constraint abtid
        );""")

        print("... ... bestellung")
        cur.execute("""create table bestellung ( 
        bestnr int auto_increment primary key, -- automatische Hochzählen aktivieren
        bestelldatum TIMESTAMP,
        bearbeitet_email varchar(100) not null,  -- min-Kardinalität 1
        foreign key (bearbeitet_email) references mitarbeiter(email) on delete cascade, -- FK-Constraint
        kunde varchar(100) not null,  -- min-Kardinalität 1
        foreign key (kunde) references kunde(email) on delete cascade, -- FK-Constraint
        statusid int not null, -- min-Kardinalität 1
        foreign key (statusid) references status(statusid) on delete cascade -- FK-Constraint
        );""")

        print("... ... warenkorbposition")
        cur.execute("""create table warenkorbposition (
        kunde_email varchar(100), -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
        foreign key (kunde_email) references kunde(email) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
        pnr int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
        foreign key (pnr) references produkt(pnr) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
        menge int,
        primary key (kunde_email, pnr) -- Definition eines zusammengesetzten Primärschlüssels
        );""")

        print("... ... bestellposition")
        cur.execute("""create table bestellposition (
        bestnr int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
        foreign key (bestnr) references bestellung(bestnr) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
        pnr int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
        foreign key (pnr) references produkt(pnr) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
        menge int,
        primary key (bestnr, pnr) -- Definition eines zusammengesetzten Primärschlüssels, 8);
        );""")

        print("... ... gemeinsamerkauf")
        cur.execute("""create table gemeinsamerkauf (
        k1_katid int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
        foreign key (k1_katid) references kategorie(katid) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
        k2_katid int, -- wird Teil des Primärschlüssels (s.u.), daher NOT NULL nicht nötig
        foreign key (k2_katid) references kategorie(katid) on delete cascade, -- gleichzeitig Teil vom Primärschlüssel und Fremdschlüssel
        primary key (k1_katid, k2_katid) -- Definition eines zusammengesetzten Primärschlüssel1, 2);
        );
        """)

        print("... finished with DDL.")

        conn.close()


def insert_rows(tunnel, table_name, rows, hostname, username, password, database, cols=None, delete_table=True):
    """ inserts the provided rows into the tabe table_name using the database connection conn
        - hostname, username, password, and database name required
        - rows needs to be a standard python list filled with tuples
        - if cols is provided, the column names are inserted into the insert-statement 
          otherwise values for all columns are assumed
        - if delete_table is set to True, the table is emptied before inserting new values
    """
    with tunnel:
        conn = mysql.connector.connect(host=hostname, user=username, passwd=password, autocommit=True, database=database, auth_plugin='mysql_native_password')
        cur = conn.cursor()
        
        # only delete if flag is set
        if delete_table:
            cur.execute(f"delete from {table_name}")
            
        # iterate over all provided rows
        for r in rows:
            if cols:
                # specific columns inserted
                cur.execute(f"insert into {table_name} {cols} values {r}")
            else:
                # all columns inserted
                cur.execute(f"insert into {table_name} values {r}")
        print(f"... ... inserted {len(rows)} rows into table {table_name}")
        conn.close()


def random_date(start, end):
    """ This function will return a random datetime between two datetime objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


def fill_database(tunnel, hostname, username, password, database):
    """ fill the database (DML) with (partially) random values """
    print(f"Filling database {database}...")

    print(f"... table status")
    table_name = 'status'
    states = [(1, 'Bestellt'), 
            (2, 'Kommissioniert'), 
            (3, 'Versendet'), 
            (4, 'Zugestellt'), 
            (5, 'Retoure')
            ]

    insert_rows(tunnel, table_name, states, hostname, username, password, database)

    print(f"... table kategorie")
    table_name = 'kategorie'
    categories = [(1, 'Molkereiprodukte'), 
                (2, 'Eier'), 
                (3, 'Süßwaren'), 
                (4, 'Obst'), 
                (5, 'Gemüse'),
                (6, 'Fleisch'),
                (7, 'Fisch')  # dieser Kategorie wird kein Produkt zugewiesen
            ]

    insert_rows(tunnel, table_name, categories, hostname, username, password, database)

    
    table_name = 'produkt'
    # Produkte mit Kategoriezuordnung
    print(f"... table produkt (mit Kategorie)")
    products_1 = [(1, 'Schokoladenpudding', '2020-11-24 12:15:59', 100, 0.8, 1),
                (2, 'Eier (Freilandhaltung)', '2020-11-24 13:16:59', 300, 2.2, 2),
                (3, 'Banane', '2020-11-24 14:10:55', 150, 0.23, 4),
                (4, 'Zitrone', '2020-11-25 11:13:59', 80, 0.34, 4),
                (5, 'Blumenkohl', '2020-10-24 12:15:59', 10, 1.8, 5),
                (6, 'Grillwurst', '2020-10-24 12:14:48', 50, 3.6, 6),
                (7, 'Weiße Schokolade', '2020-12-01 09:15:59', 90, 1.1, 3),
                (8, 'Zartbitterschokolade', '2020-11-28 16:15:59', 106, 0.9, 3),
                (9, 'Vollmilch', '2020-09-24 13:13:23', 80, 0.9, 1)            
            ]

    insert_rows(tunnel, table_name, products_1, hostname, username, password, database)

    # Produkte ohne Kategoriezuordnung
    print(f"... table produkt (ohne Kategorie)")
    products_2 = [(10, 'Katzenfutter', '2020-11-30 12:17:42', 10, 2.20), 
                (11, 'Hundefutter', '2020-11-30 12:17:48', 18, 2.40)]

    insert_rows(tunnel, table_name, products_2, hostname, username, password, database, cols="(pnr, bezeichnung, angelegt_am, verfuegbarkeit, preis)", delete_table=False)

    products = products_1
    products.extend(products_2)


    table_name = 'abteilung'
    print(f"... table abteilung (ohne Elternknoten)")
    ous_without_parent = [(1, 'Unternehmensleitung')]
    insert_rows(tunnel, table_name, ous_without_parent, hostname, username, password, database, cols="(abtid, bezeichnung)")

    print(f"... table abteilung (mit Elternknoten)")
    ous_with_parent = [(2, 'Vertrieb', 1),
        (3, 'Einkauf', 1),
        (4, 'Strategischer Einkauf', 3),
        (5, 'Operativer Einkauf', 4)        
            ]

    insert_rows(tunnel, table_name, ous_with_parent, hostname, username, password, database, delete_table=False)


    table_name = 'benutzer'
    print(f"... table benutzer")
    users = [('peter.schulz@ab.de', 'geheim', 'Peter', 'Schulz'), 
            ('julia.meier@ab.de', 'geheim', 'Julia', 'Meier'),
            ('hans.mueller@xy.de', 'geheim', 'Hans', 'Müller'),
            ('lea.schmidt@xy.de', 'geheim', 'Lea', 'Schmidt'),
            ('leonie.hausmann@ms.de', 'geheim', 'Leonie', 'Hausmann'),
            ('fr.wiedel@ms.de', 'geheim', 'Friedrich', 'Wiedel'),
            ]

    insert_rows(tunnel, table_name, users, hostname, username, password, database)


    table_name = 'kunde'
    print(f"... table kunde")
    customers = [('peter.schulz@ab.de', 1, 'Hauptstr.', '1a', '76185', 'Karlsruhe', '1990-09-19'), 
                ('hans.mueller@xy.de', 2, 'Meisenweg', '12', '45472', 'Essen', '1961-04-13'),
                ('lea.schmidt@xy.de', 3, 'Schillerstr.', '75', '76135', 'Karlsruhe', '1992-11-11'),
                ('leonie.hausmann@ms.de', 4, 'Marktplatz', '14', '48459', 'Münster', '1988-01-23'),
                ]

    insert_rows(tunnel, table_name, customers, hostname, username, password, database)

    table_name = 'mitarbeiter'
    print(f"... table mitarbeiter")
    employees = [('julia.meier@ab.de', 1, '0721 5556655', 2),
                ('fr.wiedel@ms.de', 2, '0721 5556656', 4),
                ]

    insert_rows(tunnel, table_name, employees, hostname, username, password, database)

    table_name = 'warenkorbposition'
    print(f"... table warenkorbposition")
    basket_positions = []

    for c in customers:
        # draw random number --> number of products in basket for this customer
        random_number_of_products = randint(1, len(products)+1)
        
        # draw without replacement from products
        chosen_products = choice(len(products), random_number_of_products, replace=False)
        
        # add products to basket
        for product_idx in chosen_products:
            product_id = products[product_idx][0] # get product id        
            amount = randint(1, 100) # draw random number --> amount of product in basket
            
            # customer email (c[0]), product_id, amount
            basket_positions.append((c[0], product_id, amount))

    insert_rows(tunnel, table_name, basket_positions, hostname, username, password, database)


    # Bestellungen und Bestellpositionen
    orders = []
    order_positions = []

    # start and end dates of orders (for random choice)
    d1 = datetime.strptime('8/1/2020 1:30 PM', '%m/%d/%Y %I:%M %p')
    d2 = datetime.strptime('11/25/2020 4:50 AM', '%m/%d/%Y %I:%M %p')

    order_number = 1  # we will not use auto_increment here (easier referencing)

    for c in customers:
        # draw random number --> number of orders
        number_of_orders = randint(1, 20)
        
        for order_idx in range(number_of_orders):
            
            # get random employee
            employee_email = employees[randint(1, len(employees))][0]
            
            # get random state
            state = states[randint(1, len(states))][0]
                    
            # create order  - order number (not using auto_increment), random date, employee email, customer email, status
            orders.append((order_number, random_date(d1, d2).strftime("%Y-%m-%d %H:%M:%S"), employee_email, c[0], state))
            

            # draw random number --> number of products in order for this customer
            random_number_of_products = randint(1, len(products)+1)

            # draw without replacement from products
            chosen_products = choice(len(products), random_number_of_products, replace=False)

            # add products to order
            for product_idx in chosen_products:
                product_id = products[product_idx][0] # get product id        
                amount = randint(1, 100) # draw random number --> amount of product in order

                order_positions.append((order_number, product_id, amount))
                
            # increase order number
            order_number += 1

    print(f"... table bestellung")
    insert_rows(tunnel, 'bestellung', orders, hostname, username, password, database)
    print(f"... table bestellposition")
    insert_rows(tunnel, 'bestellposition', order_positions, hostname, username, password, database)


    table_name = 'gemeinsamerkauf'
    print(f"... table gemeinsamerkauf")
    correlated_sales_categories = [(1, 2), # Molkereiprodukte und Eier
                                (4, 5), # Obst und Gemüse
                                (3, 6)  # Süßwaren und Fleisch
                                ]

    insert_rows(tunnel, table_name, correlated_sales_categories, hostname, username, password, database)

    print("... finished with DML.")



def reset_and_fill_database(tunnel, hostname, username, password, database):
    """ execute all DDL and DML statements sequentially """
    reset_database(tunnel, hostname, username, password, database)
    fill_database(tunnel, hostname, username, password, database)