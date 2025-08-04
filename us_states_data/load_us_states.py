from __future__ import annotations

from sqlmodel import SQLModel, Session, create_engine, select
from settings import settings
from models import State
from data import states
import pandas as pd

from prefect import flow, task

@task(retries=3, retry_delay_seconds=[2, 5, 15])
def fetch_states() -> list[State]:
    """Fetch the list of US states."""
    # In a real-world scenario, this could be replaced with an API call or database query
    print("Fetching US states data...")
    return states

@task
def to_dataframe(states: list[State]) -> pd.DataFrame:
    """Convert list of State objects to a dataframe."""
    print("Converting states to DataFrame...")
    states_dicts = [state.model_dump() for state in states]
    df = pd.DataFrame(states_dicts)
    return df

@task
def save_to_database(df: pd.DataFrame) -> None:
    """Save the list of State objects to the database."""
    print("Saving states to the database...")
    engine = create_engine(settings.database_url)
    SQLModel.metadata.create_all(engine)

    states = [State(**row) for row in df.to_dict(orient="records")]

    with Session(engine) as session:
        # Insert data only if table is empty
        if not session.exec(select(State)).all():
            session.add_all(states)
            session.commit()
            print(f"Inserted {len(states)} records successfully.")
        else:
            print("Data already exists.")

@flow(name="ETL US States Data", log_prints=True)
def etl():
    """Run the ETL process to load US states data."""
    states = fetch_states()
    df = to_dataframe(states)
    save_to_database(df)


if __name__ == "__main__":
    etl()
