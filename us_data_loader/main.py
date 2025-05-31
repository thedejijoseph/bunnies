from sqlmodel import SQLModel, Session, create_engine, select
from settings import settings
from models import State
from data import states


def main():
    engine = create_engine(settings.database_url)

    # Create tables
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        # Insert data only if table is empty
        if not session.exec(select(State)).all():
            session.add_all(states)
            session.commit()
            print("Data inserted successfully.")
        else:
            print("Data already exists.")


if __name__ == "__main__":
    main()
