import polars as pl
from polars import Date


def remove_markup(expr: pl.Expr) -> pl.Expr:
    return (
        pl.when(expr.is_not_null())
        .then(
            expr.cast(pl.String)
            .str.replace_all("&lt;", "<")
            .str.replace_all("&gt", ">")
            .str.replace_all(r"<[^>]*>", "")
            .str.replace_all(r"[\'\"]", "")
        )
        .otherwise(None)
    )


def normalise_identifier(expr: pl.Expr) -> pl.Expr:
    return (
        pl.when(expr.is_not_null())
        .then(expr.cast(pl.String).str.to_lowercase().str.strip_chars().str.replace_all(r"^https?://[^/]+/", ""))
        .otherwise(None)
    )


def normalise_isni(expr: pl.Expr) -> pl.Expr:
    return expr.str.replace_all(" ", "").str.strip_chars()


def extract_orcid(expr: pl.Expr) -> pl.Expr:
    return pl.when(expr.is_not_null()).then(expr.str.extract(r"\d{4}-\d{4}-\d{4}-\d{4}")).otherwise(None)


def date_parts_to_date(expr: pl.Expr) -> pl.Expr:
    year = expr.list.get(0, null_on_oob=True)
    month = expr.list.get(1, null_on_oob=True)
    day = expr.list.get(2, null_on_oob=True)

    return (
        pl.when(expr.is_null())
        .then(None)
        .when(year.is_not_null() & month.is_not_null() & day.is_not_null())
        .then(pl.datetime(year, month, day, ambiguous="null").cast(Date))
        .when(year.is_not_null() & month.is_not_null())
        .then(pl.datetime(year, month, 1, ambiguous="null").cast(Date).dt.month_end())
        .when(year.is_not_null())
        .then(pl.datetime(year, 12, 31, ambiguous="null").cast(Date))
        .otherwise(None)
    )


def make_page(first_page: pl.Expr, last_page: pl.Expr) -> pl.Expr:
    return (
        pl.when(first_page.is_null() | last_page.is_null())
        .then(None)
        .otherwise(pl.concat_str([first_page, pl.lit("-"), last_page]))
    )
