from meetup_pipeline.config.datasets import RAW_DATASETS


def test_raw_datasets_are_defined():
    assert RAW_DATASETS
    assert isinstance(RAW_DATASETS, list)


def test_raw_datasets_have_required_keys():
    required_keys = {"file_name", "table_name"}

    for dataset in RAW_DATASETS:
        assert required_keys.issubset(dataset.keys())


def test_raw_dataset_file_names_are_csv():
    for dataset in RAW_DATASETS:
        assert dataset["file_name"].endswith(".csv")


def test_raw_dataset_table_names_are_uppercase():
    for dataset in RAW_DATASETS:
        assert dataset["table_name"] == dataset["table_name"].upper()