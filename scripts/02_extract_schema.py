import pathlib
import duckdb


def main() -> None:
    root = pathlib.Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    prod_db = str(data_dir / "prod_openflights.db")
    out_md = data_dir / "schema_for_prompt.md"

    with duckdb.connect(prod_db, read_only=True) as con:
        df = con.sql("DESCRIBE;").df()
        md = df.to_markdown(index=False)

    out_md.write_text(md)
    print(f"Wrote schema markdown to: {out_md}")


if __name__ == "__main__":
    main()
