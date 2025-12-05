import polars as pl

import polars_labrodq_extension  # noqa: F401


def main():

    df = pl.DataFrame(
        {
            "col1": [1, 2, None],
            "col2": [10, 20, 30],
            "col3": [1, None, 3],
        }
    )

    context = {
        "dataset_name": "test_2025_01",
        "dq_threshold": 0.15,
    }

    results = df.dq.run_yaml("/polars-labrodq-extension/test.yml")
    print(results)
    report = df.dq.quality_report_yaml("/polars-labrodq-extension/test.yml")
    print(report)


if __name__ == "__main__":
    main()
