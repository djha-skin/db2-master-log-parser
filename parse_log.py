# Copyright 2023 Daniel Jay Haskin
# IBM DB2 Master log parser script

import re
import sys
import csv


# 115 + 9 + 9 = 133
FIELD_WIDTHS = [9, 9, 115]

months = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def db2_log_to_csv(log_file, csv_file):
    """
    Parse a fixed-field log file coming from IBM DB2 and convert it to a CSV
    file.
    """
    find_date = re.compile(
        r"^---- (?P<day>\w+), +(?P<dom>\d+) +(?P<month>\w+) (?P<year>\d+) +----$"
    )
    find_number = re.compile(r"^\d+$")
    # Continuations of lines are delimited by three-digit numbers.
    # The number appears in `first` (after it is stripped) if `first` does
    # not describe a time.
    # Continuations are first created with a message ending in a space followed
    # by a three digit number.
    # They are continued when the `first` message has the same continuation
    # number.
    continuation_lines = {}

    # Open a CSV file for writing.
    with open(csv_file, "w") as f:

        time = None
        day = None
        month = None
        year = None
        writer = csv.DictWriter(f, fieldnames=["timestamp", "stc", "message"])
        writer.writeheader()
        with open(log_file, "r") as g:
            skip = g.readline()
            skip = g.readline()
            line_number = 2
            for line in g.readlines():
                line = line.replace("\0", " ")
                line_number = line_number + 1
                first = line[: FIELD_WIDTHS[0]]

                second = line[
                    FIELD_WIDTHS[0] : FIELD_WIDTHS[0] + FIELD_WIDTHS[1]
                ]
                third = line[FIELD_WIDTHS[0] + FIELD_WIDTHS[1] :].strip()

                if first[1] == " ":
                    if first.strip() == "":
                        # This is a disgusting corner case.
                        # But I don't know what else to do.
                        writer.writerow(
                            {
                                "timestamp": f"{year}-{month}-{day}T{time}",
                                "stc": second.strip(),
                                "message": third.strip(),
                            }
                        )
                        continue
                    continuation_number = int(first.strip())
                    continuation = third.strip()
                    if continuation_number in continuation_lines:
                        continuation_lines[continuation_number]["message"] = (
                            continuation_lines[continuation_number]["message"]
                            + " "
                            + continuation
                        )
                    else:
                        raise Exception(
                            " ".join(
                                [
                                    f"Continuation #{continuation_number}",
                                    "not found in continuation_lines.",
                                ]
                            )
                        )
                    continue
                else:
                    time = first.strip().replace(".", ":")

                if third[0:4] == "----":
                    date = find_date.match(third).groupdict()
                    year = int(date["year"])
                    month = months[date["month"].upper()]
                    day = int(date["dom"])
                elif third[-4] == " " and find_number.match(third[-3:]):
                    continuation_number = int(third[-3:])
                    start_of_line = third[:-4]
                    if continuation_number in continuation_lines:
                        writer.writerow(
                            continuation_lines[continuation_number]
                        )
                    continuation_lines[continuation_number] = {
                        "timestamp": f"{year}-{month}-{day}T{time}",
                        "stc": second.strip(),
                        "message": start_of_line.strip(),
                    }
                else:
                    writer.writerow(
                        {
                            "timestamp": f"{year}-{month}-{day}T{time}",
                            "stc": second.strip(),
                            "message": third.strip(),
                        }
                    )

            # Flush remaining lines.
            for number, line in continuation_lines.items():
                writer.writerow(line)


if __name__ == "__main__":
    db2_log_to_csv(sys.argv[1], sys.argv[2])