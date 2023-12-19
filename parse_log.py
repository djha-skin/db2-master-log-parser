# Copyright 2023 Daniel Jay Haskin
# IBM DB2 Master log parser script

import re
import sys
import csv
#import pprint


# 115 + 9 + 9 = 133
FIELD_WIDTHS = [9, 9, 115]

MONTHS = {
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

find_msgid = re.compile(r"""^                      # start of line
                            (?P<msgclass>[*]?[A-Z]{3}) # message class
                            (?P<msgid>\w+) +     # message id
                            (?P<message>.*)$       # The rest of the line
                        """,
                        re.VERBOSE)
find_subsystem = re.compile(r"^=(?P<subsystem>D\w\w\w) +(?P<rest>.*)$")

def db2_csv_row(year, month, day, time, stc, more):
    """
    Convert a DB2 log line to a CSV row.
    """
    matched = find_msgid.match(more)
    if matched is None:
        returned = {"msgclass": "", "msgid": "", "message": more}
    else:
        returned = matched.groupdict()
    returned["message"] = returned["message"].strip("\r\n \t")

    sub_matched = find_subsystem.match(returned["message"])
    if sub_matched is None:
        returned["subsystem"] = ""
    else:
        returned["subsystem"] = sub_matched.group("subsystem")
        returned["message"] = sub_matched.group("rest")

    returned.update(
        {
            "timestamp": f"{year:04d}-{month:02d}-{day:02d} {time}",
            "stc": stc,
        }
    )
    #pprint.pprint(more)
    #pprint.pprint(returned)
    return returned


def process_line(line, next_line, continuation_lines, writer, row_write_time_data):
    ate_next_line = (next_line is None)
    line = line.replace("\0", " ")
    line_number = line_number + 1
    first = line[: FIELD_WIDTHS[0]]

    second = line[
        FIELD_WIDTHS[0] : FIELD_WIDTHS[0] + FIELD_WIDTHS[1]
    ].strip(
        "\r\n \t"
    )
    third = line[FIELD_WIDTHS[0] + FIELD_WIDTHS[1] :].strip(
        "\r\n \t"
    )

    if next_line is not None:
        # Weird continuation line. 31 spaces, then 4 characters.
        space_continuation = True
        for i in range(0,10):
            if next_line[i] != " ":
                space_continuation = False
                break

    if space_continuation:
        return process_line(line + " " + next_line.strip("\r\n \t"), None,
                     continuation_lines, writer, row_write_time_data)

    if first[1] == " ":
        if first.strip("\r\n \t") == "":
            # This is a disgusting corner case.
            # But I don't know what else to do.
            writer.writerow(
                    db2_csv_row(
                        row_write_time_data["year"],
                        row_write_time_data["month"],
                        row_write_time_data["day"],
                        row_write_time_data["time"],
                        second,
                        third))
            return ate_next_line

        continuation_number = int(first.strip("\r\n \t"))
        continuation = third
        if continuation_number in continuation_lines:
            continuation_lines[continuation_number]["more"] = (
                continuation_lines[continuation_number]["more"]
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
        return ate_next_line
    else:
        row_write_time_data["time"] = first.strip("\r\n \t").replace(".", ":")

    if third[0:4] == "----":
        date = find_date.match(third).groupdict()
        row_write_time_data["year"] = int(date["year"])
        row_write_time_data["month"] = MONTHS[date["month"].upper()]
        row_write_time_data["day"] = int(date["dom"])
    elif third[-4] == " " and find_number.match(third[-3:]):
        continuation_number = int(third[-3:])
        start_of_line = third[:-4]
        if continuation_number in continuation_lines:
            writer.writerow(
                db2_csv_row(
                    **continuation_lines[continuation_number])
            )
        continuation_lines[continuation_number] = {
            "day": row_write_time_data["day"],
            "month": row_write_time_data["month"],
            "year": row_write_time_data["year"],
            "time": row_write_time_data["time"],
            "stc": second,
            "more": start_of_line.strip("\r\n \t"),
        }
    else:
        writer.writerow(
            db2_csv_row(year, month, day, time, second, third)
        )
    return ate_next_line

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
        writer = csv.DictWriter(
            f,
            delimiter="\t",
            fieldnames=["timestamp", "stc", "msgclass", "msgid", "subsystem", "message"],
            lineterminator="\n",
        )
        writer.writeheader()
        with open(log_file, "r") as g:
            readline()
            g.readline()
            line_number = 2
            line = None
            ate_next_line = False
            row_write_time_data = {
            }
            ate_next_line = False

            for next_line in g.readlines():
                if line is None:
                    line = next_line
                    continue
                if ate_next_line:
                    ate_next_line = False
                    line = next_line
                    continue

                # We must never from this call site call this function
                # with a None line. That is how the function knows
                # that it ate the next line.
                ate_next_line = process_line(line, next_line,
                             continuation_lines, writer, row_write_time_data)

                line = next_line
            process_line(line, None, continuation_lines, writer,
                         row_write_time_data)

            # Flush remaining lines.
            for number, line in continuation_lines.items():
                writer.writerow(
                        db2_csv_row(**line))


if __name__ == "__main__":
    db2_log_to_csv(sys.argv[1], sys.argv[2])