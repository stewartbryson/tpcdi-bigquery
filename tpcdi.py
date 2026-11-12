import logging
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer
from google.cloud import bigquery, storage
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="A utility for loading TPC-DI generated files into BigQuery.")
console = Console()
logger = logging.getLogger(__name__)

# Options shared across commands. Each can be set with an environment variable instead.
ProjectOpt = Annotated[
    str | None,
    typer.Option(envvar="TPCDI_PROJECT", help="GCP project. Defaults to the Application Default Credentials project."),
]
DatasetOpt = Annotated[str, typer.Option(envvar="TPCDI_DATASET", help="BigQuery dataset to load tables into.")]
LocationOpt = Annotated[str, typer.Option(envvar="TPCDI_LOCATION", help="Location for the dataset and bucket.")]
BucketOpt = Annotated[str, typer.Option(envvar="TPCDI_BUCKET", help="GCS bucket used as the stage.")]
PrefixOpt = Annotated[
    str, typer.Option(envvar="TPCDI_PREFIX", help="Path prefix in the bucket; acts as the stage name.")
]


def get_bq_client(project: str | None, location: str) -> bigquery.Client:
    return bigquery.Client(project=project, location=location)


def get_bucket(project: str | None, bucket: str) -> storage.Bucket:
    return storage.Client(project=project).bucket(bucket)


def delete_stage_objects(bucket: storage.Bucket, prefix: str) -> int:
    blobs = list(bucket.list_blobs(prefix=f"{prefix}/"))
    for blob in blobs:
        blob.delete()
    return len(blobs)


@app.command(
    help="Create the bucket if needed and empty the stage prefix. Mostly useful while developing this utility."
)
def recreate_stage(
    bucket: BucketOpt,
    prefix: PrefixOpt = "tpcdi",
    project: ProjectOpt = None,
    location: LocationOpt = "US",
):
    client = storage.Client(project=project)
    if client.lookup_bucket(bucket) is None:
        client.create_bucket(bucket, location=location)
        print(f"Bucket {bucket} created.")
    count = delete_stage_objects(client.bucket(bucket), prefix)
    print(f"Stage gs://{bucket}/{prefix} recreated ({count} objects removed).")


@app.command(help="Delete everything under the stage prefix. Useful when all the data has been successfully loaded.")
def drop_stage(
    bucket: BucketOpt,
    prefix: PrefixOpt = "tpcdi",
    project: ProjectOpt = None,
):
    count = delete_stage_objects(get_bucket(project, bucket), prefix)
    print(f"Stage gs://{bucket}/{prefix} dropped ({count} objects removed).")


@app.command(help="DROP a dataset and all of its tables. Useful for cleaning up after a demo.")
def drop_dataset(
    dataset: DatasetOpt = "tpcdi",
    project: ProjectOpt = None,
    location: LocationOpt = "US",
):
    client = get_bq_client(project, location)
    client.delete_dataset(f"{client.project}.{dataset}", delete_contents=True, not_found_ok=True)
    print(f"Dataset {dataset} dropped.")


# Helpers for building BigQuery schemas that read like the original Snowpark StructTypes
def req(name: str, field_type: str) -> bigquery.SchemaField:
    return bigquery.SchemaField(name, field_type, mode="REQUIRED")


def opt(name: str, field_type: str) -> bigquery.SchemaField:
    return bigquery.SchemaField(name, field_type, mode="NULLABLE")


@app.command(help="Upload a file or files into the stage and build the dependent tables.")
def process_files(
    bucket: BucketOpt,
    input_directory: Annotated[
        str, typer.Option(help="Directory containing the DIGen.jar generated files.")
    ] = "digen-files",
    file_name: Annotated[
        str,
        typer.Option(
            help="The TPC-DI file name to upload and process. Pass value 'FINWIRE' to process all of the financial wire files."
        ),
    ] = "all",
    prefix: PrefixOpt = "tpcdi",
    dataset: DatasetOpt = "tpcdi",
    project: ProjectOpt = None,
    location: LocationOpt = "US",
    batch: Annotated[
        int, typer.Option(help="The TPC-DI batch number to process. Currently only supports the default of '1'.")
    ] = 1,
    overwrite: Annotated[bool, typer.Option(help="Overwrite the file even if it exists?")] = False,
    skip_upload: Annotated[bool, typer.Option(help="Skip uploading the files?")] = False,
    show: Annotated[
        bool,
        typer.Option(
            help="Show a sample of the data instead of saving it as a table? Writes to a '<table>__preview' table that expires in one hour."
        ),
    ] = False,
):
    client = get_bq_client(project, location)
    gcs_bucket = get_bucket(project, bucket)
    dataset_id = f"{client.project}.{dataset}"

    # create the dataset if it doesn't exist
    client.create_dataset(bigquery.Dataset(dataset_id), exists_ok=True)

    def get_stage_path(file_name: str) -> str:
        return f"{prefix}/Batch{batch}/{file_name}"

    def table_id(table_name: str) -> str:
        # when showing, write to a throwaway preview table instead of the real one
        return f"{dataset_id}.{table_name}__preview" if show else f"{dataset_id}.{table_name}"

    # method to control printing the table or reporting it was saved
    def finish_table(table_name: str):
        if show:
            table = client.get_table(table_id(table_name))
            table.expires = datetime.now(UTC) + timedelta(hours=1)
            client.update_table(table, ["expires"])

            rows = client.list_rows(table, max_results=10)
            output = Table(*[field.name for field in table.schema])
            for row in rows:
                output.add_row(*["" if value is None else str(value) for value in row.values()])
            console.print(output)
        else:
            print(f"{table_name.upper()} table created.")

    # method for uploading files
    # returns the delimiter and the list of GCS URIs for the files
    def upload_files(file_name: str) -> tuple[str, list[str]]:
        delimiter = "|"

        if file_name == "FINWIRE":
            pathlist = sorted(Path(input_directory).glob(f"Batch{batch}/FINWIRE??????"))
        else:
            pathlist = sorted(Path(input_directory).glob(f"Batch{batch}/{file_name}"))

        if not pathlist:
            raise typer.BadParameter(f"No files matching '{file_name}' found in {input_directory}/Batch{batch}.")

        uris = []
        for file in pathlist:
            # capture the delimiter
            if file_name != "FINWIRE":
                logger.info(f"Suffix: {file.suffix}")
                if file.suffix == ".csv":
                    delimiter = ","
                logger.info(f"Delimiter: {delimiter}")

            blob = gcs_bucket.blob(get_stage_path(file.name))
            uris.append(f"gs://{gcs_bucket.name}/{blob.name}")

            # put the file(s) in the stage
            if not skip_upload:
                if blob.exists() and not overwrite:
                    print(f"File {file.name}: SKIPPED")
                else:
                    blob.upload_from_filename(str(file))
                    print(f"File {file.name}: UPLOADED")

        return delimiter, uris

    # method for creating a table from a simple CSV
    # works for either comma or pipe delimited, detecting that automatically by suffix
    def load_csv(
        schema: list[bigquery.SchemaField],
        file_name: str,
        table_name: str,
    ):
        delimiter, uris = upload_files(file_name)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=delimiter,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.load_table_from_uri(uris, table_id(table_name), job_config=job_config).result()
        finish_table(table_name)

    # method for creating a table from a SELECT statement
    def save_query(select_sql: str, table_name: str):
        client.query_and_wait(f"CREATE OR REPLACE TABLE `{table_id(table_name)}` AS\n{select_sql}")
        finish_table(table_name)

    ### Start defining and loading the actual tables
    ### Variable 'con_file_name' declaration acts as the comment.

    con_file_name = "Date.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("SK_DATE_ID", "INT64"),
            req("DATE_VALUE", "DATE"),
            req("DATE_DESC", "STRING"),
            req("CALENDAR_YEAR_ID", "INT64"),
            req("CALENDAR_YEAR_DESC", "STRING"),
            req("CALENDAR_QTR_ID", "INT64"),
            req("CALENDAR_QTR_DESC", "STRING"),
            req("CALENDAR_MONTH_ID", "INT64"),
            req("CALENDAR_MONTH_DESC", "STRING"),
            req("CALENDAR_WEEK_ID", "INT64"),
            req("CALENDAR_WEEK_DESC", "STRING"),
            req("DAY_OF_WEEK_NUM", "INT64"),
            req("DAY_OF_WEEK_DESC", "STRING"),
            req("FISCAL_YEAR_ID", "INT64"),
            req("FISCAL_YEAR_DESC", "STRING"),
            req("FISCAL_QTR_ID", "INT64"),
            req("FISCAL_QTR_DESC", "STRING"),
            req("HOLIDAY_FLAG", "BOOL"),
        ]
        load_csv(schema, con_file_name, "date")

    con_file_name = "DailyMarket.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("DM_DATE", "DATE"),
            req("DM_S_SYMB", "STRING"),
            req("DM_CLOSE", "FLOAT64"),
            req("DM_HIGH", "FLOAT64"),
            req("DM_LOW", "FLOAT64"),
            req("DM_VOL", "FLOAT64"),
        ]
        load_csv(schema, con_file_name, "daily_market")

    con_file_name = "Industry.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("IN_ID", "STRING"),
            req("IN_NAME", "STRING"),
            req("IN_SC_ID", "STRING"),
        ]
        load_csv(schema, con_file_name, "industry")

    con_file_name = "Prospect.csv"
    if file_name in ["all", con_file_name]:
        schema = [
            req("AGENCY_ID", "STRING"),
            opt("LAST_NAME", "STRING"),
            opt("FIRST_NAME", "STRING"),
            opt("MIDDLE_INITIAL", "STRING"),
            opt("GENDER", "STRING"),
            opt("ADDRESS_LINE1", "STRING"),
            opt("ADDRESS_LINE2", "STRING"),
            opt("POSTAL_CODE", "STRING"),
            opt("CITY", "STRING"),
            opt("STATE", "STRING"),
            opt("COUNTRY", "STRING"),
            opt("PHONE", "STRING"),
            opt("INCOME", "INT64"),
            opt("NUMBER_CARS", "INT64"),
            opt("NUMBER_CHILDREN", "INT64"),
            opt("MARITAL_STATUS", "STRING"),
            opt("AGE", "INT64"),
            opt("CREDIT_RATING", "INT64"),
            opt("OWN_OR_RENT_FLAG", "STRING"),
            opt("EMPLOYER", "STRING"),
            opt("NUMBER_CREDIT_CARDS", "INT64"),
            opt("NET_WORTH", "INT64"),
        ]
        load_csv(schema, con_file_name, "prospect")

    con_file_name = "CustomerMgmt.xml"
    if file_name in ["all", con_file_name]:
        upload_files(con_file_name)

        # BigQuery has no native XML reader, so parse the local file and load the rows as JSON
        rows = parse_customer_mgmt(Path(input_directory) / f"Batch{batch}" / con_file_name)

        schema = [
            req("ACTION_TS", "DATETIME"),
            req("ACTION_TYPE", "STRING"),
            req("C_ID", "INT64"),
            opt("C_TAX_ID", "STRING"),
            opt("C_GNDR", "STRING"),
            opt("C_TIER", "INT64"),
            opt("C_DOB", "DATE"),
            opt("C_L_NAME", "STRING"),
            opt("C_F_NAME", "STRING"),
            opt("C_M_NAME", "STRING"),
            opt("C_ADLINE1", "STRING"),
            opt("C_ADLINE2", "STRING"),
            opt("C_ZIPCODE", "STRING"),
            opt("C_CITY", "STRING"),
            opt("C_STATE_PROV", "STRING"),
            opt("C_CTRY", "STRING"),
            opt("C_PRIM_EMAIL", "STRING"),
            opt("C_ALT_EMAIL", "STRING"),
            opt("C_PHONE_1", "STRING"),
            opt("C_PHONE_2", "STRING"),
            opt("C_PHONE_3", "STRING"),
            opt("C_LCL_TX_ID", "STRING"),
            opt("C_NAT_TX_ID", "STRING"),
            opt("CA_ID", "STRING"),
            opt("CA_TAX_ST", "INT64"),
            opt("CA_B_ID", "INT64"),
            opt("CA_NAME", "STRING"),
        ]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.load_table_from_json(rows, table_id("customer_mgmt"), job_config=job_config).result()
        finish_table("customer_mgmt")

    con_file_name = "TaxRate.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("TX_ID", "STRING"),
            opt("TX_NAME", "STRING"),
            opt("TX_RATE", "FLOAT64"),
        ]
        load_csv(schema, con_file_name, "tax_rate")

    con_file_name = "HR.csv"
    if file_name in ["all", con_file_name]:
        schema = [
            req("EMPLOYEE_ID", "INT64"),
            req("MANAGER_ID", "INT64"),
            opt("EMPLOYEE_FIRST_NAME", "STRING"),
            opt("EMPLOYEE_LAST_NAME", "STRING"),
            opt("EMPLOYEE_MI", "STRING"),
            opt("EMPLOYEE_JOB_CODE", "INT64"),
            opt("EMPLOYEE_BRANCH", "STRING"),
            opt("EMPLOYEE_OFFICE", "STRING"),
            opt("EMPLOYEE_PHONE", "STRING"),
        ]
        load_csv(schema, con_file_name, "hr")

    con_file_name = "WatchHistory.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("W_C_ID", "INT64"),
            opt("W_S_SYMB", "STRING"),
            opt("W_DTS", "DATETIME"),
            opt("W_ACTION", "STRING"),
        ]
        load_csv(schema, con_file_name, "watch_history")

    con_file_name = "Trade.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("T_ID", "INT64"),
            req("T_DTS", "DATETIME"),
            req("T_ST_ID", "STRING"),
            req("T_TT_ID", "STRING"),
            req("T_IS_CASH", "BOOL"),
            req("T_S_SYMB", "STRING"),
            req("T_QTY", "FLOAT64"),
            req("T_BID_PRICE", "FLOAT64"),
            req("T_CA_ID", "INT64"),
            req("T_EXEC_NAME", "STRING"),
            opt("T_TRADE_PRICE", "FLOAT64"),
            opt("T_CHRG", "FLOAT64"),
            opt("T_COMM", "FLOAT64"),
            opt("T_TAX", "FLOAT64"),
        ]
        load_csv(schema, con_file_name, "trade")

    con_file_name = "TradeHistory.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("TH_T_ID", "INT64"),
            req("TH_DTS", "DATETIME"),
            req("TH_ST_ID", "STRING"),
        ]
        load_csv(schema, con_file_name, "trade_history")

    con_file_name = "StatusType.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("ST_ID", "STRING"),
            req("ST_NAME", "STRING"),
        ]
        load_csv(schema, con_file_name, "status_type")

    con_file_name = "TradeType.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("TT_ID", "STRING"),
            req("TT_NAME", "STRING"),
            req("TT_IS_SELL", "BOOL"),
            req("TT_IS_MARKET", "BOOL"),
        ]
        load_csv(schema, con_file_name, "trade_type")

    con_file_name = "HoldingHistory.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("HH_H_T_ID", "INT64"),
            req("HH_T_ID", "INT64"),
            req("HH_BEFORE_QTY", "FLOAT64"),
            req("HH_AFTER_QTY", "FLOAT64"),
        ]
        load_csv(schema, con_file_name, "holding_history")

    con_file_name = "CashTransaction.txt"
    if file_name in ["all", con_file_name]:
        schema = [
            req("CT_CA_ID", "INT64"),
            req("CT_DTS", "DATETIME"),
            req("CT_AMT", "FLOAT64"),
            req("CT_NAME", "STRING"),
        ]
        load_csv(schema, con_file_name, "cash_transaction")

    con_file_name = "FINWIRE"
    if file_name in ["all", con_file_name]:
        _, uris = upload_files(con_file_name)

        # These are fixed-width fields, so read each entire line in as "LINE".
        # FINWIRE never contains '|' or quotes, so no line gets split.
        raw_table = f"{dataset_id}.finwire_raw"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="|",
            quote_character="",
            schema=[req("LINE", "STRING")],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.load_table_from_uri(uris, raw_table, job_config=job_config).result()

        # generic columns for all record types
        def finwire_select(rec_type: str, columns: dict[str, str]) -> str:
            column_sql = ",\n    ".join(f"{expr} AS {name}" for name, expr in columns.items())
            return (
                f"SELECT\n"
                f"    PARSE_DATETIME('%Y%m%d-%H%M%S', SUBSTR(LINE, 1, 15)) AS PTS,\n"
                f"    {column_sql}\n"
                f"FROM `{raw_table}`\n"
                f"WHERE SUBSTR(LINE, 16, 3) = '{rec_type}'"
            )

        # CMP record types
        save_query(
            finwire_select(
                "CMP",
                {
                    "COMPANY_NAME": "SUBSTR(LINE, 19, 60)",
                    "CIK": "SUBSTR(LINE, 79, 10)",
                    "STATUS": "SUBSTR(LINE, 89, 4)",
                    "INDUSTRY_ID": "SUBSTR(LINE, 93, 2)",
                    "SP_RATING": "SUBSTR(LINE, 95, 4)",
                    "FOUNDING_DATE": "SAFE.PARSE_DATE('%Y%m%d', TRIM(SUBSTR(LINE, 99, 8)))",
                    "ADDRESS_LINE1": "SUBSTR(LINE, 107, 80)",
                    "ADDRESS_LINE2": "SUBSTR(LINE, 187, 80)",
                    "POSTAL_CODE": "SUBSTR(LINE, 267, 12)",
                    "CITY": "SUBSTR(LINE, 279, 25)",
                    "STATE_PROVINCE": "SUBSTR(LINE, 304, 20)",
                    "COUNTRY": "SUBSTR(LINE, 324, 24)",
                    "CEO_NAME": "SUBSTR(LINE, 348, 46)",
                    "DESCRIPTION": "SUBSTR(LINE, 394, 150)",
                },
            ),
            "cmp",
        )

        # SEC record types
        save_query(
            finwire_select(
                "SEC",
                {
                    "SYMBOL": "SUBSTR(LINE, 19, 15)",
                    "ISSUE_TYPE": "SUBSTR(LINE, 34, 6)",
                    "STATUS": "SUBSTR(LINE, 40, 4)",
                    "NAME": "SUBSTR(LINE, 44, 70)",
                    "EX_ID": "SUBSTR(LINE, 114, 6)",
                    "SH_OUT": "SUBSTR(LINE, 120, 13)",
                    "FIRST_TRADE_DATE": "SUBSTR(LINE, 133, 8)",
                    "FIRST_EXCHANGE_DATE": "SUBSTR(LINE, 141, 8)",
                    "DIVIDEND": "SUBSTR(LINE, 149, 12)",
                    "CO_NAME_OR_CIK": "SUBSTR(LINE, 161, 60)",
                },
            ),
            "sec",
        )

        # FIN record types
        save_query(
            finwire_select(
                "FIN",
                {
                    "YEAR": "SUBSTR(LINE, 19, 4)",
                    "QUARTER": "SUBSTR(LINE, 23, 1)",
                    "QUARTER_START_DATE": "SUBSTR(LINE, 24, 8)",
                    "POSTING_DATE": "SUBSTR(LINE, 32, 8)",
                    "REVENUE": "SUBSTR(LINE, 40, 17)",
                    "EARNINGS": "SUBSTR(LINE, 57, 17)",
                    "EPS": "SUBSTR(LINE, 74, 12)",
                    "DILUTED_EPS": "SUBSTR(LINE, 86, 12)",
                    "MARGIN": "SUBSTR(LINE, 98, 12)",
                    "INVENTORY": "SUBSTR(LINE, 110, 17)",
                    "ASSETS": "SUBSTR(LINE, 127, 17)",
                    "LIABILITIES": "SUBSTR(LINE, 144, 17)",
                    "SH_OUT": "SUBSTR(LINE, 161, 13)",
                    "DILUTED_SH_OUT": "SUBSTR(LINE, 174, 13)",
                    "CO_NAME_OR_CIK": "SUBSTR(LINE, 187, 60)",
                },
            ),
            "fin",
        )

        # the raw table was only needed for the transformations above
        client.delete_table(raw_table, not_found_ok=True)


### XML parsing for CustomerMgmt.xml
### Elements that are present but empty become '', and elements that are absent become NULL.
### For UPDCUST actions, absent means "unchanged" while empty means "cleared".


def xml_text(parent: ET.Element | None, path: str) -> str | None:
    if parent is None:
        return None
    element = parent.find(path)
    if element is None:
        return None
    return element.text or ""


def try_int(value: str | None) -> int | None:
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def try_date(value: str | None) -> str | None:
    try:
        return date.fromisoformat(value).isoformat()
    except TypeError, ValueError:
        return None


# Constructs a phone number from its nested elements, e.g. '1-872-523-8928 ext: 123'.
# Separators are only added next to parts that have a value.
def get_phone_number(phone: ET.Element | None, separator: str = "-") -> str | None:
    if phone is None:
        return None
    ctry_code = xml_text(phone, "C_CTRY_CODE") or ""
    area_code = xml_text(phone, "C_AREA_CODE") or ""
    local = xml_text(phone, "C_LOCAL") or ""
    ext = xml_text(phone, "C_EXT") or ""
    return (
        ctry_code
        + ("" if ctry_code == "" else separator)
        + area_code
        + ("" if area_code == "" else separator)
        + local
        + ("" if ext == "" else " ext: ")
        + ext
    )


def parse_customer_mgmt(path: Path) -> list[dict]:
    action_tag = "{http://www.tpc.org/tpc-di}Action"
    rows = []

    for _, action in ET.iterparse(path, events=("end",)):
        if action.tag != action_tag:
            continue

        customer = action.find("Customer")
        name = customer.find("Name")
        address = customer.find("Address")
        contact_info = customer.find("ContactInfo")
        tax_info = customer.find("TaxInfo")
        account = customer.find("Account")

        rows.append(
            {
                # Action attributes
                "ACTION_TS": action.get("ActionTS").replace("T", " "),
                "ACTION_TYPE": action.get("ActionType"),
                # Customer attributes
                "C_ID": int(customer.get("C_ID")),
                "C_TAX_ID": customer.get("C_TAX_ID"),
                "C_GNDR": customer.get("C_GNDR"),
                "C_TIER": try_int(customer.get("C_TIER")),
                "C_DOB": try_date(customer.get("C_DOB")),
                # Name elements
                "C_L_NAME": xml_text(name, "C_L_NAME"),
                "C_F_NAME": xml_text(name, "C_F_NAME"),
                "C_M_NAME": xml_text(name, "C_M_NAME"),
                # Address elements
                "C_ADLINE1": xml_text(address, "C_ADLINE1"),
                "C_ADLINE2": xml_text(address, "C_ADLINE2"),
                "C_ZIPCODE": xml_text(address, "C_ZIPCODE"),
                "C_CITY": xml_text(address, "C_CITY"),
                "C_STATE_PROV": xml_text(address, "C_STATE_PROV"),
                "C_CTRY": xml_text(address, "C_CTRY"),
                # Contact Info elements
                "C_PRIM_EMAIL": xml_text(contact_info, "C_PRIM_EMAIL"),
                "C_ALT_EMAIL": xml_text(contact_info, "C_ALT_EMAIL"),
                # Construct phone numbers from multi-nested elements
                "C_PHONE_1": get_phone_number(contact_info.find("C_PHONE_1") if contact_info is not None else None),
                "C_PHONE_2": get_phone_number(contact_info.find("C_PHONE_2") if contact_info is not None else None),
                "C_PHONE_3": get_phone_number(contact_info.find("C_PHONE_3") if contact_info is not None else None),
                # TaxInfo elements
                "C_LCL_TX_ID": xml_text(tax_info, "C_LCL_TX_ID"),
                "C_NAT_TX_ID": xml_text(tax_info, "C_NAT_TX_ID"),
                # Account attributes
                "CA_ID": account.get("CA_ID") if account is not None else None,
                "CA_TAX_ST": try_int(account.get("CA_TAX_ST")) if account is not None else None,
                # Account elements
                "CA_B_ID": try_int(xml_text(account, "CA_B_ID")),
                "CA_NAME": xml_text(account, "CA_NAME"),
            }
        )

        # free memory as we go
        action.clear()

    return rows


if __name__ == "__main__":
    app()
