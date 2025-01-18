from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Set random seed for reproducibility
random.seed(42)
fake = Faker()
Faker.seed(42)


def generate_fake_orders(
    num_orders: int = 1000, num_customers: int = 200
) -> pd.DataFrame:
    """Generate fake order data"""

    # Generate list of unique customer names
    customers = [fake.name() for _ in range(num_customers)]

    # Product options
    products = ["transfer", "parking", "fast_track", "lounge"]

    # Calculate date range (last 6 months from 2023-12-10)
    end_date = datetime(2023, 12, 10)
    start_date = end_date - timedelta(days=180)

    # Generate orders
    orders = []
    for i in range(num_orders):
        order = {
            "customer_name": random.choice(customers),
            "order_id": str(100 + i),  # Order ID as string to match test data
            "price": round(random.uniform(1, 100), 2),
            "order_date": fake.date_between(
                start_date=start_date, end_date=end_date
            ).strftime(
                "%Y-%m-%d"
            ),  # Order date as YYYY-MM-DD
            "product_id": random.choice(products),
            "passenger_count": random.randint(1, 6),
        }
        orders.append(order)

    # Convert to DataFrame
    df = pd.DataFrame(orders)
    return df


def save_data(df: pd.DataFrame, output_dir: str = "data") -> None:
    """Save DataFrame to CSV and Parquet formats"""

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Save as CSV
    csv_path = os.path.join(output_dir, "raw_orders.csv")
    df.to_csv(csv_path, index=False)
    print(f"Saved CSV to: {csv_path}")

    # Save as Parquet
    parquet_path = os.path.join(output_dir, "raw_orders.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"Saved Parquet to: {parquet_path}")


def main():
    """Main function to generate and save fake data"""
    print("Generating fake order data...")
    df = generate_fake_orders()

    print("\nSample of generated data:")
    print(df.head())

    print("\nDataset Info:")
    print(f"Total orders: {len(df)}")
    print(f"Unique customers: {df['customer_name'].nunique()}")
    print(f"Date range: {df['order_date'].min()} to {df['order_date'].max()}")

    print("\nSaving data...")
    save_data(df)


if __name__ == "__main__":
    main()
