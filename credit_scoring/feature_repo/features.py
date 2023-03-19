from datetime import timedelta
from feast import (
    Entity, Feature,
    FeatureService, FeatureView,
    FileSource, ValueType, Field
)
import feast.types
import os

zipcode = Entity(
    name="zipcode",
    join_keys=["zipcode"]
)
zipcode_source = FileSource(
    path=os.path.abspath("raw_data/zipcode_table.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)
zipcode_features = FeatureView(
    name="zipcode_features",
    entities=[zipcode],
    ttl=timedelta(days=36500),
    schema=[
        Field(name="city", dtype=feast.types.String),
        Field(name="state", dtype=feast.types.String),
        Field(name="location_type", dtype=feast.types.String),
        Field(name="tax_returns_filed", dtype=feast.types.Int64),
        Field(name="population", dtype=feast.types.Int64),
        Field(name="total_wages", dtype=feast.types.Int64)
    ],
    online=True,
    source=zipcode_source,
    tags={
        "owner": "luanht1",
        "team": "Ops"
    }
)

dob_ssn = Entity(
    name="dob_ssn",
    join_keys=["dob_ssn"],
    description="Date of birth and last four digits of social security number..."
)
credit_history_source = FileSource(
    path=os.path.abspath("raw_data/credit_history.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)
credit_history = FeatureView(
    name="credit_history",
    entities=[dob_ssn],
    ttl=timedelta(days=36500),
    schema=[
        Field(name="credit_card_due", dtype=feast.types.Int64),
        Field(name="mortgage_due", dtype=feast.types.Int64),
        Field(name="student_loan_due", dtype=feast.types.Int64),
        Field(name="vehicle_loan_due", dtype=feast.types.Int64),
        Field(name="hard_pulls", dtype=feast.types.Int64),
        Field(name="missed_payments_2y", dtype=feast.types.Int64),
        Field(name="missed_payments_1y", dtype=feast.types.Int64),
        Field(name="missed_payments_6m", dtype=feast.types.Int64),
        Field(name="bankruptcies", dtype=feast.types.Int64)
    ],
    online=True,
    source=credit_history_source,
    tags={
        "owner": "luanht1",
        "team": "Ops"
    }
)

features_group_v1 = FeatureService(
    name="features_group_v1",
    features=[
        credit_history, 
        zipcode_features
    ]
)

features_group_v2 = FeatureService(
    name="features_group_v2",
    features=[
        credit_history,
        zipcode_features[["city"]]
    ]
)







