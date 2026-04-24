from meetup_pipeline.ingestion.normalization import normalize_csv_bytes_to_utf8


def test_normalize_utf8_csv_bytes():
    csv_bytes = "id,name\n1,Cristian\n".encode("utf-8")

    normalized_text, detected_encoding = normalize_csv_bytes_to_utf8(csv_bytes)

    assert normalized_text == "id,name\r\n1,Cristian\r\n"
    assert detected_encoding == "utf-8-sig"


def test_normalize_latin1_csv_bytes_to_utf8():
    csv_bytes = "id,name\n1,José\n".encode("latin-1")

    normalized_text, detected_encoding = normalize_csv_bytes_to_utf8(csv_bytes)

    assert normalized_text == "id,name\r\n1,José\r\n"
    assert "José" in normalized_text
    assert detected_encoding == "cp1252"

