import csv
import io

CANDIDATE_ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "iso-8859-1"]

def normalize_csv_bytes_to_utf8(raw_bytes: bytes) -> tuple[str, str]:
  
    decoded_text = None
    detected_encoding = None

    for encoding in CANDIDATE_ENCODINGS:
        try:
            decoded_text = raw_bytes.decode(encoding, errors="strict")
            detected_encoding = encoding
            break
        except UnicodeDecodeError:
            continue

    if decoded_text is None:
        
        decoded_text = raw_bytes.decode("utf-8", errors="replace")
        detected_encoding = "utf-8-replaced"

    input_buffer = io.StringIO(decoded_text)
    output_buffer = io.StringIO(newline="")

    reader = csv.reader(input_buffer)
    writer = csv.writer(output_buffer, quoting=csv.QUOTE_MINIMAL)

    for row in reader:
        writer.writerow(row)

    return output_buffer.getvalue(), detected_encoding