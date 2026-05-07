import json
import psycopg2
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, RuntimeContext

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "csv-data"

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "mysecretpassword"

UPSERT_PET = """
INSERT INTO dim_customer_pet (customer_pet_type, customer_pet_name, customer_pet_breed)
VALUES (%s,%s,%s)
ON CONFLICT (customer_pet_type, customer_pet_name, customer_pet_breed) DO NOTHING
RETURNING customer_pet_id
"""

UPSERT_CUSTOMER = """
INSERT INTO dim_customer (customer_first_name, customer_last_name, customer_age,
                          customer_email, customer_country, customer_postal_code,
                          customer_pet_id, pet_category)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (customer_first_name, customer_last_name, customer_email) DO UPDATE
SET customer_age = EXCLUDED.customer_age,
    customer_country = EXCLUDED.customer_country,
    customer_postal_code = EXCLUDED.customer_postal_code,
    customer_pet_id = EXCLUDED.customer_pet_id,
    pet_category = EXCLUDED.pet_category
RETURNING sale_customer_id
"""

UPSERT_SELLER = """
INSERT INTO dim_seller (seller_first_name, seller_last_name, seller_email,
                        seller_country, seller_postal_code)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (seller_first_name, seller_last_name, seller_email) DO UPDATE
SET seller_country = EXCLUDED.seller_country,
    seller_postal_code = EXCLUDED.seller_postal_code
RETURNING sale_seller_id
"""

UPSERT_SUPPLIER = """
INSERT INTO dim_supplier (supplier_name, supplier_contact, supplier_email, supplier_phone,
                          supplier_address, supplier_city, supplier_country)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (supplier_name, supplier_email) DO UPDATE
SET supplier_contact = EXCLUDED.supplier_contact,
    supplier_phone = EXCLUDED.supplier_phone,
    supplier_address = EXCLUDED.supplier_address,
    supplier_city = EXCLUDED.supplier_city,
    supplier_country = EXCLUDED.supplier_country
RETURNING product_supplier_id
"""

UPSERT_PRODUCT = """
INSERT INTO dim_product (product_supplier_id, product_name, product_category,
                         product_price, product_quantity, product_weight, product_color,
                         product_size, product_brand, product_material, product_description,
                         product_rating, product_reviews, product_release_date, product_expiry_date)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (product_name, product_price, product_brand, product_color, product_size, product_material, product_supplier_id) DO UPDATE
SET product_category = EXCLUDED.product_category,
    product_quantity = EXCLUDED.product_quantity,
    product_weight = EXCLUDED.product_weight,
    product_description = EXCLUDED.product_description,
    product_rating = EXCLUDED.product_rating,
    product_reviews = EXCLUDED.product_reviews,
    product_release_date = EXCLUDED.product_release_date,
    product_expiry_date = EXCLUDED.product_expiry_date
RETURNING sale_product_id
"""

UPSERT_STORE = """
INSERT INTO dim_store (store_name, store_location, store_city, store_state,
                       store_country, store_phone, store_email)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (store_email) DO UPDATE
SET store_name = EXCLUDED.store_name,
    store_location = EXCLUDED.store_location,
    store_city = EXCLUDED.store_city,
    store_state = EXCLUDED.store_state,
    store_country = EXCLUDED.store_country,
    store_phone = EXCLUDED.store_phone
RETURNING sale_store_id
"""

INSERT_FACT = """
INSERT INTO fact_sales (sale_product_id, sale_seller_id, sale_customer_id, sale_store_id,
                        sale_quantity, sale_total_price, sale_date)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""


def _get_one(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None


class StarSchemaMapper(MapFunction):
    def open(self, ctx: RuntimeContext):
        self.conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                     user=PG_USER, password=PG_PASSWORD)

    def close(self):
        if self.conn:
            self.conn.close()

    def map(self, value: str):
        try:
            rec = json.loads(value)
        except Exception as e:
            print(f"JSON parse error: {e}")
            return

        # 1. pet
        pet_id = _get_one(self.conn, UPSERT_PET,
                          (rec.get("customer_pet_type"),
                           rec.get("customer_pet_name"),
                           rec.get("customer_pet_breed")))

        # 2. customer
        cust_id = _get_one(self.conn, UPSERT_CUSTOMER,
                           (rec.get("customer_first_name"),
                            rec.get("customer_last_name"),
                            int(rec["customer_age"]) if rec.get("customer_age") else None,
                            rec.get("customer_email"),
                            rec.get("customer_country"),
                            rec.get("customer_postal_code"),
                            pet_id,
                            rec.get("pet_category")))

        # 3. seller
        seller_id = _get_one(self.conn, UPSERT_SELLER,
                             (rec.get("seller_first_name"),
                              rec.get("seller_last_name"),
                              rec.get("seller_email"),
                              rec.get("seller_country"),
                              rec.get("seller_postal_code")))

        # 4. supplier
        supp_id = _get_one(self.conn, UPSERT_SUPPLIER,
                           (rec.get("supplier_name"),
                            rec.get("supplier_contact"),
                            rec.get("supplier_email"),
                            rec.get("supplier_phone"),
                            rec.get("supplier_address"),
                            rec.get("supplier_city"),
                            rec.get("supplier_country")))

        # 5. product
        prod_id = _get_one(self.conn, UPSERT_PRODUCT,
                           (supp_id,
                            rec.get("product_name"),
                            rec.get("product_category"),
                            float(rec["product_price"]) if rec.get("product_price") else None,
                            int(rec["product_quantity"]) if rec.get("product_quantity") else None,
                            float(rec["product_weight"]) if rec.get("product_weight") else None,
                            rec.get("product_color"),
                            rec.get("product_size"),
                            rec.get("product_brand"),
                            rec.get("product_material"),
                            rec.get("product_description"),
                            float(rec["product_rating"]) if rec.get("product_rating") else None,
                            int(rec["product_reviews"]) if rec.get("product_reviews") else None,
                            rec.get("product_release_date"),
                            rec.get("product_expiry_date")))

        # 6. store
        store_id = _get_one(self.conn, UPSERT_STORE,
                            (rec.get("store_name"),
                             rec.get("store_location"),
                             rec.get("store_city"),
                             rec.get("store_state"),
                             rec.get("store_country"),
                             rec.get("store_phone"),
                             rec.get("store_email")))

        if all([prod_id, seller_id, cust_id, store_id]):
            with self.conn.cursor() as cur:
                cur.execute(INSERT_FACT, (
                    prod_id, seller_id, cust_id, store_id,
                    int(rec["sale_quantity"]) if rec.get("sale_quantity") else None,
                    float(rec["sale_total_price"]) if rec.get("sale_total_price") else None,
                    rec.get("sale_date")
                ))
                self.conn.commit()
                print(f"Fact inserted: {rec.get('sale_customer_id')} - {rec.get('product_name')}")
        else:
            print(f"Missing dimension, skip fact: prod={prod_id}, seller={seller_id}, cust={cust_id}, store={store_id}")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.add_jars(
        "file:///opt/flink/extra-jars/flink-sql-connector-kafka-3.0.2-1.18.jar",
        "file:///opt/flink/extra-jars/postgresql-42.7.3.jar",
        "file:///opt/flink/extra-jars/flink-connector-jdbc-3.1.2-1.18.jar"
    )

    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "flink-star-group",
        "auto.offset.reset": "earliest"
    }
    consumer = FlinkKafkaConsumer(KAFKA_TOPIC, SimpleStringSchema(), kafka_props)

    stream = env.add_source(consumer)
    stream.map(StarSchemaMapper())

    env.execute("Star Schema Streaming")


if __name__ == "__main__":
    main()
