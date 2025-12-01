
DATABASE_CONFIG = {
    "name": "shopez",
    "table_name": "orders",
    "delta_path": "/delta/shopez/orders",
    "checkpoint_location": "/delta/shopez/checkpoints"
}


INITIAL_SCHEMA = {
    "order_id": "string",
    "order_timestamp": "timestamp",
    "customer_id": "string", 
    "country": "string",
    "amount": "double",
    "currency": "string",
    "status": "string"
}

EVOLVED_SCHEMA = {
    **INITIAL_SCHEMA,
    "payment_method": "string",
    "coupon_code": "string"
}


PARTITION_COLUMNS = ["country", "order_date"]


COUNTRIES = ["US", "IN", "UK", "DE", "FR", "JP", "CA", "AU", "BR", "SG"]
CURRENCIES = {
    "US": "USD", "IN": "INR", "UK": "GBP", "DE": "EUR", "FR": "EUR",
    "JP": "JPY", "CA": "CAD", "AU": "AUD", "BR": "BRL", "SG": "SGD"
}
ORDER_STATUSES = ["CREATED", "PAID", "CANCELLED"]
PAYMENT_METHODS = ["CARD", "UPI", "COD", "WALLET", "BANK_TRANSFER"]
COUPON_CODES = ["SAVE10", "WELCOME20", "FIRST50", None]


OPTIMIZATION_CONFIG = {
    "zorder_columns": ["customer_id"],
    "optimize_schedule": "DAILY",
    "vacuum_retention_hours": 168,  # 7 days
    "auto_optimize": True
}


QUALITY_RULES = {
    "min_amount": 0.01,
    "max_amount": 10000.0,
    "required_fields": ["order_id", "customer_id", "country", "amount"],
    "valid_statuses": ORDER_STATUSES,
    "valid_countries": COUNTRIES
}