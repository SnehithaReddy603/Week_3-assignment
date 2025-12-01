import random
from datetime import datetime, timedelta
from typing import List, Tuple

class OrderDataGenerator:
    
    def __init__(self):
        self.countries = ["US", "IN", "UK", "DE", "FR", "JP", "CA", "AU", "BR", "SG"]
        self.currencies = {
            "US": "USD", "IN": "INR", "UK": "GBP", "DE": "EUR", "FR": "EUR",
            "JP": "JPY", "CA": "CAD", "AU": "AUD", "BR": "BRL", "SG": "SGD"
        }
        self.statuses = ["CREATED", "PAID", "CANCELLED"]
        self.payment_methods = ["CARD", "UPI", "COD", "WALLET", "BANK_TRANSFER"]
        self.coupon_codes = ["SAVE10", "WELCOME20", "FIRST50", None, None, None]
    
    def generate_orders(self, count: int, start_date: datetime = None, 
                       include_extended_fields: bool = False) -> List[Tuple]:
        if start_date is None:
            start_date = datetime(2024, 1, 1)
        
        orders = []
        for i in range(count):
            country = random.choice(self.countries)
            order_data = [
                f"ORD{i+1:08d}",
                start_date + timedelta(
                    days=random.randint(0, 120),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                ),
                f"CUST{random.randint(1, 10000):06d}",
                country,
                round(random.uniform(5.0, 1000.0), 2),
                self.currencies[country],
                random.choice(self.statuses)
            ]
            
            if include_extended_fields:
                order_data.extend([
                    random.choice(self.payment_methods),
                    random.choice(self.coupon_codes)
                ])
            
            orders.append(tuple(order_data))
        
        return orders
    
    def generate_small_file_scenario(self, count: int = 100) -> List[Tuple]:
        orders = []
        for i in range(count):
            orders.append((
                f"SMALL{i:03d}",
                datetime(2024, 6, 1) + timedelta(hours=i),
                f"CUST{i:06d}",
                f"XX{i%50:02d}",  # Many different countries
                50.0,
                "USD",
                "CREATED"
            ))
        return orders

class DataQualityValidator:
    
    @staticmethod
    def validate_order_data(df, spark):
        checks = []
        

        null_check = df.select([
            sum(col(c).isNull().cast("int")).alias(f"{c}_nulls") 
            for c in ["order_id", "customer_id", "country", "amount"]
        ]).collect()[0]
        

        amount_stats = df.select(
            min("amount").alias("min_amount"),
            max("amount").alias("max_amount")
        ).collect()[0]
        

        total_orders = df.count()
        unique_orders = df.select("order_id").distinct().count()
        
        return {
            "null_checks": dict(null_check.asDict()),
            "amount_range": dict(amount_stats.asDict()),
            "duplicate_orders": total_orders - unique_orders,
            "total_records": total_orders
        }