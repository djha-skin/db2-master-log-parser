# Copyright 2023 Daniel Jay Haskin
# IBM DB2 Master log parser script

import re
import sys
import csv

# import pprint


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


class DB2LogParser:
    """
    Parses DB2 Log Lines.
    """

    find_number = re.compile(r"^\d+$")
    find_msgid = re.compile(
        r"""^                      # start of line
                                (?P<msgclass>[*]?[A-Z]{3}) # message class
                                (?P<msgid>\w+) +     # message id
                                (?P<message>.*)$       # The rest of the line
                            """,
        re.VERBOSE,
    )
    find_subsystem = re.compile(r"^=(?P<subsystem>D\w\w\w) +(?P<rest>.*)$")
    find_date = re.compile(
        r"^---- (?P<day>\w+), +(?P<dom>\d+) +(?P<month>\w+) (?P<year>\d+) +----$"
    )

    def __init__(self, csv_file_handle, log_file_handle):
        self.writer = csv.DictWriter(
            csv_file_handle,
            delimiter="\t",
            fieldnames=[
                "timestamp",
                "stc",
                "msgclass",
                "msgid",
                "subsystem",
                "message",
            ],
            lineterminator="\n",
        )
        self.reader = log_file_handle
        self.line_number = 0
        self.current_line_time = None
        self.current_line_year = None
        self.current_line_month = None
        self.current_line_day = None
        self.continuation_lines = {}
        self.current_line = None
        self.next_line = None

    @staticmethod
    def _db2_csv_row(year, month, day, time, stc, more):
        """
        Convert a DB2 log line to a CSV row.
        """
        matched = DB2LogParser.find_msgid.match(more)
        if matched is None:
            returned = {"msgclass": "", "msgid": "", "message": more}
        else:
            returned = matched.groupdict()
        returned["message"] = returned["message"].strip("\r\n \t")

        sub_matched = DB2LogParser.find_subsystem.match(returned["message"])
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
        # pprint.pprint(more)
        # pprint.pprint(returned)
        return returned

    def _flush_continuation_lines(self, writer):
        """
        Flush remaining lines.
        """
        for number, line in self.continuation_lines.items():
            writer.writerow(DB2LogParser._db2_csv_row(**line))

    def _attempt_process_line(self, line):
        next_line = self.reader.readline()
        line = line.replace("\0", " ")
        line.strip("\r\n")
        self.line_number = self.line_number + 1

        first = line[: FIELD_WIDTHS[0]]

        second = line[
            FIELD_WIDTHS[0] : FIELD_WIDTHS[0] + FIELD_WIDTHS[1]
        ].strip("\r\n \t")
        third = line[FIELD_WIDTHS[0] + FIELD_WIDTHS[1] :].strip("\r\n \t")

        if next_line != "":
            # Weird continuation line. 31 spaces, then 4 characters.
            space_continuation = True
            for i in range(0, 10):
                if next_line[i] != " ":
                    space_continuation = False
                    break

        if space_continuation:
            return self._attempt_process_line(
                line + " " + next_line.strip("\r\n \t")
            )
        if first[1] == " ":
            if first.strip("\r\n \t") == "":
                # This is a disgusting corner case.
                # But I don't know what else to do.
                writer.writerow(
                    DB2LogParser._db2_csv_row(
                        self.current_line_year,
                        self.current_line_month,
                        self.current_line_day,
                        self.current_line_time,
                        second,
                        third,
                    )
                )
                return next_line

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
            return next_line
        else:
            self.current_line_time = first.strip("\r\n \t").replace(".", ":")

        if third[0:4] == "----":
            date = DB2LogParser.find_date.match(third).groupdict()
            self.current_line_year = int(date["year"])
            self.current_line_month = MONTHS[date["month"].upper()]
            self.current_line_day = int(date["dom"])
        elif third[-4] == " " and DB2LogParser.find_number.match(third[-3:]):
            continuation_number = int(third[-3:])
            start_of_line = third[:-4]
            if continuation_number in continuation_lines:
                writer.writerow(
                    DB2LogParser._db2_csv_row(
                        **continuation_lines[continuation_number]
                    )
                )
            continuation_lines[continuation_number] = {
                "day": self.current_line_day,
                "month": self.current_line_month,
                "year": self.current_line_year,
                "time": self.current_line_time,
                "stc": second,
                "more": start_of_line.strip("\r\n \t"),
            }
        else:
            writer.writerow(
                DB2LogParser._db2_csv_row(
                    year, month, day, time, second, third
                )
            )
        return next_line

    def parse(self):
        self.reader.readline()
        self.reader.readline()
        self.line_number = 2
        self.writer.writeheader()
        line = self.reader.readline()
        while line != "":
            line = self._attempt_process_line(line)
        self._flush_continuation_lines(self.writer)


def db2_log_to_csv(log_file, csv_file):
    """
    Parse a fixed-field log file coming from IBM DB2 and convert it to a CSV
    file.
    """
    # Continuations of lines are delimited by three-digit numbers.
    # The number appears in `first` (after it is stripped) if `first` does
    # not describe a time.
    # Continuations are first created with a message ending in a space followed
    # by a three digit number.
    # They are continued when the `first` message has the same continuation
    # number.

    # Open a CSV file for writing.
    with open(csv_file, "w") as c:
        with open(log_file, "r") as l:
            parser = DB2LogParser(c, l)
            parser.parse()


if __name__ == "__main__":
    db2_log_to_csv(sys.argv[1], sys.argv[2])