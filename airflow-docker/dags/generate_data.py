from datetime import datetime, timedelta
import os
import random
import subprocess

from faker import Faker

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="generate_data",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "ddl", "ecommerce"],
)
def generate_data():
    def _max_id(cur, table: str, id_col: str) -> int:
        cur.execute(f"SELECT COALESCE(MAX({id_col}), 0) FROM {table}")
        return int(cur.fetchone()[0])

    def _fetch_int_list(cur, query: str) -> list[int]:
        cur.execute(query)
        return [int(r[0]) for r in cur.fetchall()]

    def _executemany(conn, cur, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        cur.executemany(sql, rows)
        conn.commit()

    create_schema = SQLExecuteQueryOperator(
        task_id="create_ecommerce_tables_and_indexes",
        conn_id="postgres",
        autocommit=True,
        sql="""
        CREATE TABLE IF NOT EXISTS categories (
            category_id      INT PRIMARY KEY,
            category_name    VARCHAR(100) NOT NULL,
            parent_id        INT,
            category_code    VARCHAR(50),
            description      TEXT,
            is_active        BOOLEAN DEFAULT TRUE,
            sort_order       INT,
            seo_title        VARCHAR(255),
            seo_description  VARCHAR(255),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id       INT PRIMARY KEY,
            product_name     VARCHAR(255) NOT NULL,
            sku              VARCHAR(100),
            category_id      INT,
            brand            VARCHAR(100),
            base_price       DECIMAL(15, 2),
            cost_price       DECIMAL(15, 2),
            currency         VARCHAR(10) DEFAULT 'USD',
            status           VARCHAR(50),
            weight           DECIMAL(10, 2),
            length           DECIMAL(10, 2),
            width            DECIMAL(10, 2),
            height           DECIMAL(10, 2),
            color            VARCHAR(50),
            size             VARCHAR(50),
            material         VARCHAR(100),
            supplier_name    VARCHAR(255),
            warranty_months  INT,
            release_date     DATE,
            tags             VARCHAR(255),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_products_category
                FOREIGN KEY (category_id) REFERENCES categories(category_id)
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id          INT PRIMARY KEY,
            full_name        VARCHAR(255),
            email            VARCHAR(255) UNIQUE,
            phone            VARCHAR(20),
            address          TEXT,
            city             VARCHAR(100),
            state            VARCHAR(100),
            country          VARCHAR(100),
            postal_code      VARCHAR(20),
            gender           VARCHAR(20),
            date_of_birth    DATE,
            loyalty_segment  VARCHAR(50),
            registration_source VARCHAR(100),
            preferred_language  VARCHAR(10),
            preferred_currency  VARCHAR(10),
            is_active        BOOLEAN DEFAULT TRUE,
            last_login_at    TIMESTAMP,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id         INT PRIMARY KEY,
            user_id          INT,
            order_date       TIMESTAMP,
            total_amount     DECIMAL(15, 2),
            subtotal_amount  DECIMAL(15, 2),
            tax_amount       DECIMAL(15, 2),
            shipping_fee     DECIMAL(10, 2),
            discount_amount  DECIMAL(15, 2),
            status           VARCHAR(50),
            payment_method   VARCHAR(50),
            payment_status   VARCHAR(50),
            shipping_address TEXT,
            billing_address  TEXT,
            coupon_code      VARCHAR(50),
            source_channel   VARCHAR(100),
            device_type      VARCHAR(50),
            browser          VARCHAR(100),
            ip_address       VARCHAR(50),
            user_agent       TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_orders_user
                FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id    INT PRIMARY KEY,
            order_id         INT,
            product_id       INT,
            quantity         INT,
            unit_price       DECIMAL(15, 2),
            discount_amount  DECIMAL(15, 2),
            tax_amount       DECIMAL(15, 2),
            line_total       DECIMAL(15, 2),
            variant_name     VARCHAR(255),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_order_items_order
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
            CONSTRAINT fk_order_items_product
                FOREIGN KEY (product_id) REFERENCES products(product_id)
        );

        CREATE TABLE IF NOT EXISTS promotions (
            promo_id         INT PRIMARY KEY,
            promo_code       VARCHAR(50),
            discount_percent DECIMAL(5, 2),
            discount_type    VARCHAR(50),
            start_date       DATE,
            end_date         DATE,
            min_order_value  DECIMAL(15, 2),
            max_discount_amount DECIMAL(15, 2),
            campaign_name    VARCHAR(255),
            channel_limit    VARCHAR(100),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS order_promotions (
            order_id         INT,
            promo_id         INT,
            applied_amount   DECIMAL(15, 2),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_id, promo_id),
            CONSTRAINT fk_order_promotions_order
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
            CONSTRAINT fk_order_promotions_promo
                FOREIGN KEY (promo_id) REFERENCES promotions(promo_id)
        );

        CREATE TABLE IF NOT EXISTS warehouses (
            warehouse_id     INT PRIMARY KEY,
            warehouse_name   VARCHAR(100),
            location         VARCHAR(255),
            capacity         INT,
            manager_name     VARCHAR(255),
            phone            VARCHAR(20),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS inventory (
            inventory_id     INT PRIMARY KEY,
            product_id       INT,
            warehouse_id     INT,
            stock_on_hand    INT,
            reserved_quantity INT,
            reorder_level    INT,
            reorder_quantity INT,
            safety_stock     INT,
            last_restock_date TIMESTAMP,
            next_restock_date TIMESTAMP,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_inventory_product
                FOREIGN KEY (product_id) REFERENCES products(product_id),
            CONSTRAINT fk_inventory_warehouse
                FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
        );

        CREATE TABLE IF NOT EXISTS marketing_channels (
            channel_id       INT PRIMARY KEY,
            channel_name     VARCHAR(100),
            channel_type     VARCHAR(50),
            platform         VARCHAR(100),
            utm_source       VARCHAR(100),
            utm_medium       VARCHAR(100),
            utm_campaign     VARCHAR(100),
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS clickstream (
            event_id         BIGINT PRIMARY KEY,
            user_id          INT,
            session_id       VARCHAR(100),
            event_type       VARCHAR(50),
            product_id       INT,
            category_id      INT,
            channel_id       INT,
            timestamp        TIMESTAMP,
            page_url         TEXT,
            referrer_url     TEXT,
            ad_campaign      VARCHAR(255),
            device_type      VARCHAR(50),
            browser          VARCHAR(100),
            os               VARCHAR(100),
            country          VARCHAR(100),
            city             VARCHAR(100),
            price_viewed     DECIMAL(15, 2),
            discount_shown   DECIMAL(5, 2),
            is_new_user      BOOLEAN,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_clickstream_user
                FOREIGN KEY (user_id) REFERENCES users(user_id),
            CONSTRAINT fk_clickstream_product
                FOREIGN KEY (product_id) REFERENCES products(product_id),
            CONSTRAINT fk_clickstream_category
                FOREIGN KEY (category_id) REFERENCES categories(category_id),
            CONSTRAINT fk_clickstream_channel
                FOREIGN KEY (channel_id) REFERENCES marketing_channels(channel_id)
        );

        CREATE TABLE IF NOT EXISTS watermark (
            table_name VARCHAR(100) PRIMARY KEY,
            last_watermark TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        INSERT INTO watermark (table_name, last_watermark)
        VALUES
            ('categories', CURRENT_TIMESTAMP),
            ('products', CURRENT_TIMESTAMP),
            ('users', CURRENT_TIMESTAMP),
            ('orders', CURRENT_TIMESTAMP),
            ('order_items', CURRENT_TIMESTAMP),
            ('promotions', CURRENT_TIMESTAMP),
            ('order_promotions', CURRENT_TIMESTAMP),
            ('warehouses', CURRENT_TIMESTAMP),
            ('inventory', CURRENT_TIMESTAMP),
            ('marketing_channels', CURRENT_TIMESTAMP),
            ('clickstream', CURRENT_TIMESTAMP)
        ON CONFLICT (table_name) DO NOTHING;

        CREATE INDEX IF NOT EXISTS idx_orders_user_date
            ON orders (user_id, order_date);

        CREATE INDEX IF NOT EXISTS idx_order_items_product
            ON order_items (product_id);

        CREATE INDEX IF NOT EXISTS idx_inventory_product
            ON inventory (product_id);

        CREATE INDEX IF NOT EXISTS idx_clickstream_user_time
            ON clickstream (user_id, timestamp);

        CREATE INDEX IF NOT EXISTS idx_clickstream_event_type
            ON clickstream (event_type);

        CREATE INDEX IF NOT EXISTS idx_clickstream_product
            ON clickstream (product_id);

        CREATE INDEX IF NOT EXISTS idx_orders_updated_at
            ON orders (updated_at);

        CREATE INDEX IF NOT EXISTS idx_products_updated_at
            ON products (updated_at);

        CREATE INDEX IF NOT EXISTS idx_inventory_updated_at
            ON inventory (updated_at);
        """,
    )

    @task
    def seed_categories(n: int = 200) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "categories", "category_id")

                rows = []
                for i in range(1, n + 1):
                    category_id = start_id + i
                    category_name = faker.unique.word().title()
                    category_code = f"CAT-{category_id:04d}"
                    is_active = random.random() < 0.9
                    rows.append((category_id, category_name, None, category_code, None, is_active))

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO categories (
                        category_id, category_name, parent_id, category_code, description, is_active
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    @task
    def generate_clickstream_csv() -> None:
        os.makedirs("/opt/spark-data", exist_ok=True)
        env = os.environ.copy()
        env["CSV_OUT_DIR"] = "/opt/spark-data"
        subprocess.run(
            ["python", "/opt/airflow/spark_scripts/generate_clickstream_csv.py"],
            check=True,
            env=env,
        )

    @task
    def seed_marketing_channels(n: int = 8) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        channel_names = [
            "Google Ads",
            "Facebook Ads",
            "Email",
            "Direct",
            "Organic Search",
            "Referral",
            "TikTok Ads",
            "Affiliate",
            "Display",
            "YouTube Ads",
        ]
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "marketing_channels", "channel_id")

                rows = []
                for i in range(1, n + 1):
                    channel_id = start_id + i
                    channel_name = channel_names[(i - 1) % len(channel_names)]
                    channel_type = random.choice(["Paid", "Organic", "Referral"])
                    platform = random.choice(["Google", "Meta", "Email", "Web", "Partner"])
                    utm_source = faker.domain_word()
                    utm_medium = random.choice(["cpc", "social", "email", "organic", "referral"])
                    utm_campaign = f"camp_{faker.word()}_{faker.random_int(1, 999)}"
                    rows.append(
                        (
                            channel_id,
                            channel_name,
                            channel_type,
                            platform,
                            utm_source,
                            utm_medium,
                            utm_campaign,
                        )
                    )

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO marketing_channels (
                        channel_id, channel_name, channel_type, platform, utm_source, utm_medium, utm_campaign
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    @task
    def seed_users(n: int = 200) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        loyalty_segments = ["VIP", "Loyal", "New", "Churn"]
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "users", "user_id")

                rows = []
                for i in range(1, n + 1):
                    user_id = start_id + i
                    full_name = faker.name()
                    email = f"user{user_id}_{faker.user_name()}@{faker.free_email_domain()}"
                    phone = faker.msisdn()[:20]
                    address = faker.address()
                    city = faker.city()[:100]
                    state = faker.state()[:100]
                    country = faker.country()[:100]
                    postal_code = faker.postcode()[:20]
                    gender = random.choice(["male", "female", "other"])
                    date_of_birth = faker.date_of_birth(minimum_age=18, maximum_age=65)
                    loyalty_segment = random.choices(loyalty_segments, weights=[0.1, 0.3, 0.5, 0.1], k=1)[0]
                    registration_source = random.choice(["web", "app", "referral"])[:100]
                    preferred_language = random.choice(["en", "vi"])[:10]
                    preferred_currency = random.choice(["USD", "VND"])[:10]
                    is_active = random.random() < 0.95
                    last_login_at = faker.date_time_between(start_date="-60d", end_date="now")

                    rows.append(
                        (
                            user_id,
                            full_name,
                            email,
                            phone,
                            address,
                            city,
                            state,
                            country,
                            postal_code,
                            gender,
                            date_of_birth,
                            loyalty_segment,
                            registration_source,
                            preferred_language,
                            preferred_currency,
                            is_active,
                            last_login_at,
                        )
                    )

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO users (
                        user_id, full_name, email, phone, address, city, state, country, postal_code,
                        gender, date_of_birth, loyalty_segment, registration_source, preferred_language,
                        preferred_currency, is_active, last_login_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    @task
    def seed_products(n: int = 100) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        statuses = ["active", "inactive", "discontinued"]
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "products", "product_id")
                category_ids = _fetch_int_list(cur, "SELECT category_id FROM categories")
                if not category_ids:
                    raise ValueError("No categories found. Run seed_categories first.")

                rows = []
                for i in range(1, n + 1):
                    product_id = start_id + i
                    product_name = f"{faker.word().title()} {faker.word().title()}"
                    sku = f"SKU-{product_id:06d}"
                    category_id = random.choice(category_ids)
                    brand = random.choice(["Acme", "Globex", "Umbrella", "Initech", "Soylent", "Stark"])
                    base_price = round(random.uniform(10, 500), 2)
                    cost_price = round(base_price * random.uniform(0.4, 0.85), 2)
                    currency = random.choice(["USD", "VND"])
                    status = random.choices(statuses, weights=[0.85, 0.1, 0.05], k=1)[0]
                    weight = round(random.uniform(0.1, 10.0), 2)
                    length = round(random.uniform(5.0, 100.0), 2)
                    width = round(random.uniform(5.0, 100.0), 2)
                    height = round(random.uniform(1.0, 50.0), 2)
                    color = random.choice(["black", "white", "red", "blue", "green", "yellow"])
                    size = random.choice(["S", "M", "L", "XL", "One Size"])
                    material = random.choice(["cotton", "plastic", "metal", "leather", "wood", "glass"])
                    supplier_name = faker.company()
                    warranty_months = random.choice([0, 3, 6, 12, 24])
                    release_date = faker.date_between(start_date="-2y", end_date="today")
                    tags = ",".join({faker.word() for _ in range(random.randint(1, 3))})

                    rows.append(
                        (
                            product_id,
                            product_name,
                            sku,
                            category_id,
                            brand,
                            base_price,
                            cost_price,
                            currency,
                            status,
                            weight,
                            length,
                            width,
                            height,
                            color,
                            size,
                            material,
                            supplier_name,
                            warranty_months,
                            release_date,
                            tags,
                        )
                    )

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO products (
                        product_id, product_name, sku, category_id, brand, base_price, cost_price, currency,
                        status, weight, length, width, height, color, size, material, supplier_name,
                        warranty_months, release_date, tags
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    rows,
                )

    @task
    def seed_warehouses(n: int = 50) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "warehouses", "warehouse_id")

                rows = []
                for i in range(1, n + 1):
                    warehouse_id = start_id + i
                    warehouse_name = f"WH-{warehouse_id:03d}"[:100]
                    location = f"{faker.city()}, {faker.country()}"[:255]
                    capacity = random.randint(1000, 200000)
                    manager_name = faker.name()[:255]
                    phone = faker.msisdn()[:20]
                    rows.append((warehouse_id, warehouse_name, location, capacity, manager_name, phone))

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO warehouses (
                        warehouse_id, warehouse_name, location, capacity, manager_name, phone
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    @task
    def seed_inventory() -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "inventory", "inventory_id")
                product_ids = _fetch_int_list(cur, "SELECT product_id FROM products")
                warehouse_ids = _fetch_int_list(cur, "SELECT warehouse_id FROM warehouses")
                if not product_ids:
                    raise ValueError("No products found. Run seed_products first.")
                if not warehouse_ids:
                    raise ValueError("No warehouses found. Run seed_warehouses first.")

                rows = []
                next_id = start_id
                now = datetime.utcnow()
                for product_id in product_ids:
                    for warehouse_id in random.sample(warehouse_ids, k=random.choice([1, 2])):
                        next_id += 1
                        stock_on_hand = random.randint(0, 500)
                        reserved_quantity = random.randint(0, min(stock_on_hand, 50))
                        reorder_level = random.randint(10, 50)
                        reorder_quantity = random.randint(20, 200)
                        safety_stock = random.randint(5, 30)
                        last_restock_date = now - timedelta(days=random.randint(1, 120))
                        next_restock_date = now + timedelta(days=random.randint(1, 60))
                        rows.append(
                            (
                                next_id,
                                product_id,
                                warehouse_id,
                                stock_on_hand,
                                reserved_quantity,
                                reorder_level,
                                reorder_quantity,
                                safety_stock,
                                last_restock_date,
                                next_restock_date,
                            )
                        )

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO inventory (
                        inventory_id, product_id, warehouse_id, stock_on_hand, reserved_quantity,
                        reorder_level, reorder_quantity, safety_stock, last_restock_date, next_restock_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    @task
    def seed_promotions(n: int = 300) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "promotions", "promo_id")

                rows = []
                for i in range(1, n + 1):
                    promo_id = start_id + i
                    discount_type = random.choice(["percentage", "fixed_amount"])
                    if discount_type == "percentage":
                        discount_percent = round(random.uniform(5, 50), 2)
                    else:
                        discount_percent = None
                    promo_code = f"PROMO{promo_id:05d}"
                    end_date = faker.date_between(start_date="-30d", end_date="today")
                    start_date = end_date - timedelta(days=random.randint(7, 120))
                    min_order_value = round(random.uniform(20, 200), 2)
                    max_discount_amount = round(random.uniform(10, 150), 2)
                    campaign_name = f"{faker.word().title()} Campaign"
                    channel_limit = random.choice(["all", "paid", "organic", "email"])
                    rows.append(
                        (
                            promo_id,
                            promo_code,
                            discount_percent,
                            discount_type,
                            start_date,
                            end_date,
                            min_order_value,
                            max_discount_amount,
                            campaign_name,
                            channel_limit,
                        )
                    )

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO promotions (
                        promo_id, promo_code, discount_percent, discount_type, start_date, end_date,
                        min_order_value, max_discount_amount, campaign_name, channel_limit
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    @task
    def seed_orders(n: int = 200) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "orders", "order_id")
                user_ids = _fetch_int_list(cur, "SELECT user_id FROM users")
                if not user_ids:
                    raise ValueError("No users found. Run seed_users first.")

                rows = []
                for i in range(1, n + 1):
                    order_id = start_id + i
                    user_id = random.choice(user_ids)
                    order_date = faker.date_time_between(start_date="-365d", end_date="-180d")
                    subtotal_amount = 0
                    tax_amount = 0
                    shipping_fee = round(random.uniform(0, 30), 2)
                    discount_amount = 0
                    total_amount = 0
                    status = random.choice(["Pending", "Paid", "Shipped", "Cancelled", "Refunded"])
                    payment_method = random.choice(["COD", "Credit Card", "PayPal", "Bank Transfer"])
                    payment_status = random.choice(["paid", "unpaid", "failed"])
                    shipping_address = faker.address()
                    billing_address = faker.address()
                    coupon_code = random.choice([None, f"CPN{faker.random_int(100, 999)}"])
                    source_channel = random.choice(["direct", "email", "social", "search", "referral"])
                    device_type = random.choice(["desktop", "mobile"])
                    browser = random.choice(["Chrome", "Firefox", "Safari", "Edge"])
                    ip_address = faker.ipv4()
                    user_agent = faker.user_agent()

                    rows.append(
                        (
                            order_id,
                            user_id,
                            order_date,
                            total_amount,
                            subtotal_amount,
                            tax_amount,
                            shipping_fee,
                            discount_amount,
                            status,
                            payment_method,
                            payment_status,
                            shipping_address,
                            billing_address,
                            coupon_code,
                            source_channel,
                            device_type,
                            browser,
                            ip_address,
                            user_agent,
                        )
                    )

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO orders (
                        order_id, user_id, order_date, total_amount, subtotal_amount, tax_amount,
                        shipping_fee, discount_amount, status, payment_method, payment_status,
                        shipping_address, billing_address, coupon_code, source_channel, device_type,
                        browser, ip_address, user_agent
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

    @task
    def seed_order_items() -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "order_items", "order_item_id")
                order_ids = _fetch_int_list(cur, "SELECT order_id FROM orders")
                cur.execute("SELECT product_id, base_price FROM products")
                products = [(int(r[0]), float(r[1])) for r in cur.fetchall()]
                if not order_ids:
                    raise ValueError("No orders found. Run seed_orders first.")
                if not products:
                    raise ValueError("No products found. Run seed_products first.")

                rows = []
                next_id = start_id
                for order_id in order_ids:
                    n_lines = random.randint(1, 5)
                    for product_id, base_price in random.sample(products, k=min(n_lines, len(products))):
                        next_id += 1
                        quantity = random.randint(1, 5)
                        unit_price = round(base_price * random.uniform(0.8, 1.2), 2)
                        discount_amount = round(unit_price * quantity * random.uniform(0, 0.15), 2)
                        tax_amount = round(unit_price * quantity * random.uniform(0.02, 0.12), 2)
                        line_total = round(unit_price * quantity - discount_amount + tax_amount, 2)
                        variant_name = random.choice([None, "Color", "Size", "Bundle"])
                        rows.append(
                            (
                                next_id,
                                order_id,
                                product_id,
                                quantity,
                                unit_price,
                                discount_amount,
                                tax_amount,
                                line_total,
                                variant_name,
                            )
                        )

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO order_items (
                        order_item_id, order_id, product_id, quantity, unit_price, discount_amount,
                        tax_amount, line_total, variant_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

                cur.execute(
                    """
                    UPDATE orders o
                    SET
                        subtotal_amount = s.subtotal,
                        tax_amount = s.tax,
                        discount_amount = s.discount,
                        total_amount = (s.subtotal + s.tax + o.shipping_fee - s.discount)
                    FROM (
                        SELECT
                            order_id,
                            COALESCE(SUM(unit_price * quantity), 0) AS subtotal,
                            COALESCE(SUM(tax_amount), 0) AS tax,
                            COALESCE(SUM(discount_amount), 0) AS discount
                        FROM order_items
                        GROUP BY order_id
                    ) s
                    WHERE o.order_id = s.order_id
                    """
                )
            conn.commit()

    @task
    def seed_order_promotions() -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT order_id, subtotal_amount FROM orders")
                orders = [(int(r[0]), float(r[1] or 0)) for r in cur.fetchall()]
                cur.execute("SELECT promo_id, min_order_value, max_discount_amount FROM promotions")
                promos = [(int(r[0]), float(r[1] or 0), float(r[2] or 0)) for r in cur.fetchall()]
                if not orders:
                    raise ValueError("No orders found. Run seed_orders first.")
                if not promos:
                    raise ValueError("No promotions found. Run seed_promotions first.")

                rows = []
                for order_id, subtotal in orders:
                    if random.random() < 0.35:
                        promo_id, min_order_value, max_discount_amount = random.choice(promos)
                        if subtotal < min_order_value:
                            continue
                        applied_amount = round(min(subtotal * random.uniform(0.05, 0.25), max_discount_amount), 2)
                        rows.append((order_id, promo_id, applied_amount))

                _executemany(
                    conn,
                    cur,
                    """
                    INSERT INTO order_promotions (order_id, promo_id, applied_amount)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (order_id, promo_id) DO NOTHING
                    """,
                    rows,
                )

    @task
    def seed_clickstream(n_events: int = 3000) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")
        faker = Faker()
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                start_id = _max_id(cur, "clickstream", "event_id")
                cur.execute("SELECT user_id FROM users")
                user_ids = [int(r[0]) for r in cur.fetchall()]
                cur.execute("SELECT product_id, category_id, base_price FROM products")
                products = [(int(r[0]), int(r[1]) if r[1] is not None else None, float(r[2])) for r in cur.fetchall()]
                cur.execute("SELECT channel_id, channel_name FROM marketing_channels")
                channels = [(int(r[0]), str(r[1])) for r in cur.fetchall()]
                cur.execute("SELECT order_id, user_id, order_date FROM orders")
                order_map = {}
                for order_id, user_id, order_date in cur.fetchall():
                    if user_id is not None and order_date is not None:
                        order_map[int(user_id)] = order_map.get(int(user_id), []) + [(int(order_id), order_date)]

                if not user_ids:
                    raise ValueError("No users found. Run seed_users first.")
                if not products:
                    raise ValueError("No products found. Run seed_products first.")
                if not channels:
                    raise ValueError("No marketing channels found. Run seed_marketing_channels first.")

                rows = []
                next_id = start_id
                for _ in range(n_events):
                    next_id += 1
                    user_id = random.choice(user_ids)
                    session_id = faker.uuid4()
                    product_id, category_id, base_price = random.choice(products)
                    channel_id, channel_name = random.choice(channels)
                    device_type = random.choice(["desktop", "mobile"])
                    browser = random.choice(["Chrome", "Firefox", "Safari", "Edge"])
                    os_name = random.choice(["Windows", "macOS", "Linux", "Android", "iOS"])
                    country = faker.country()
                    city = faker.city()
                    is_new_user = random.random() < 0.2
                    discount_shown = round(random.uniform(0, 30), 2)
                    price_viewed = round(max(1.0, base_price - discount_shown), 2)
                    page_url = f"https://example.com/product/{product_id}"
                    referrer_url = f"https://{channel_name.lower().replace(' ', '')}.example.com"
                    ad_campaign = f"{channel_name} {faker.word().title()}"

                    event_type = random.choices(
                        ["view", "add_to_cart", "purchase"],
                        weights=[0.7, 0.2, 0.1],
                        k=1,
                    )[0]

                    base_ts = faker.date_time_between(start_date="-30d", end_date="now")
                    if event_type == "purchase" and user_id in order_map:
                        order_id, order_date = random.choice(order_map[user_id])
                        base_ts = order_date + timedelta(minutes=random.randint(-120, 30))

                    rows.append(
                        (
                            next_id,
                            user_id,
                            session_id,
                            event_type,
                            product_id,
                            category_id,
                            channel_id,
                            base_ts,
                            page_url,
                            referrer_url,
                            ad_campaign,
                            device_type,
                            browser,
                            os_name,
                            country,
                            city,
                            price_viewed,
                            discount_shown,
                            is_new_user,
                        )
                    )

    categories = seed_categories()
    products = seed_products()
    marketing_channels = seed_marketing_channels()
    users = seed_users()
    warehouses = seed_warehouses()
    inventory = seed_inventory()
    promotions = seed_promotions()
    orders = seed_orders()
    order_items = seed_order_items()
    order_promotions = seed_order_promotions()
    clickstream = seed_clickstream()
    clickstream_csv = generate_clickstream_csv()
    create_schema >> categories >> products
    create_schema >> marketing_channels
    create_schema >> users >> orders >> order_items >> order_promotions
    create_schema >> promotions
    promotions >> order_promotions
    create_schema >> warehouses
    [products, warehouses] >> inventory
    [categories, users, products, marketing_channels] >> clickstream
    [users, products, marketing_channels] >> clickstream_csv


generate_data()
