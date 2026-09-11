from airflow.sdk import dag, task
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
)
def demo_pipeline():

    @task
    def extract():
        print("Extracting Data...")
        return {
            "sales": [10, 20, 30],
            "users": ["Ram", "sam", "hari"]
        }

    @task
    def transform_sales(data):
        print("Transforming Sales Data...")
        return [x * 2 for x in data["sales"]]

    @task
    def transform_users(data):
        print("Transforming Users Data...")
        return [user.capitalize() for user in data["users"]]

    @task
    def load(data):
        print("Loading data...")
        print(data)

    raw_data = extract()

    transformed_sales_data = transform_sales(raw_data)

    transformed_users_data = transform_users(raw_data)

    transformed_data = {
        "sales": transformed_sales_data,
        "users": transformed_users_data
    }

    load(transformed_data)


demo_pipeline()