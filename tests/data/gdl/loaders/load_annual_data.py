"""
Load GDL HDI data from JSON
"""

import sys
from typing import List
from loader_utils import load_json, init_db_session
from api.db.models import (
    DevelopmentAnnual,
    HealthcareAnnual,
    IncomeAnnual,
    EducationAnnual,
    GdlRegion,
)


def load_annual_data(fpath: str, Model) -> List:
    print("Loading annual data from: ", fpath)
    data = load_json(fpath)
    engine, Session = init_db_session()

    loaded_ids = []
    with Session() as session:

        print("Deleting all rows in table:", Model.__table__.name)
        session.query(Model).delete()

        try:
            for record in data:
                gdl_code = record["GDLCODE"].lower()
                region_result = (
                    session.query(GdlRegion)
                    .where(GdlRegion.gdl_code == gdl_code)
                    .first()
                )
                if not region_result:
                    print("No matching gdl_code:", gdl_code)

                else:
                    for key in record:
                        if key.isnumeric():
                            value = record[key]
                            if value is not None:
                                entry = Model(
                                    gdl_code=record["GDLCODE"].lower(),
                                    year=key,
                                    value=value,
                                )
                                session.add(entry)
                                _id = session.commit()
                                loaded_ids.append(_id)

        except Exception as err:
            print(f"Data entry insert failed due to {err}, rolling back transaction...")
            session.rollback()
    engine.dispose()

    print(f"Loaded {len(loaded_ids)} records to table:", Model.__table__.name)
    return loaded_ids


def load_all_tables(development_fpath, education_fpath, income_fpath, healthcare_fpath):
    load_annual_data(development_fpath, DevelopmentAnnual)
    load_annual_data(education_fpath, EducationAnnual)
    load_annual_data(income_fpath, IncomeAnnual)
    load_annual_data(healthcare_fpath, HealthcareAnnual)


if __name__ == "__main__":
    if not len(sys.argv) == 5:
        print(
            "Usage:",
            "load_annual_data.py <development json> <education json> <income json> <healthcare json>",
        )
        sys.exit(1)

    fpath = sys.argv[1]
    if not fpath:
        print("missing fpath")
        sys.exit(1)

    load_all_tables(
        development_fpath=sys.argv[2],
        education_fpath=sys.argv[3],
        income_fpath=sys.argv[4],
        healthcare_fpath=sys.argv[5],
    )
