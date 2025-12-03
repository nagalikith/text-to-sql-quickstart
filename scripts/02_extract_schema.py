import pathlib
import psycopg2
import pandas as pd

def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_md = data_dir / "schema_for_prompt.md"

    # adjust to your container's settings
    conn = psycopg2.connect(
        dbname="canvas_development",
        user="postgres",
        password="sekret",
        host="localhost",    # or container name
        port=5433
    )

    query = """
    SELECT 
        table_name, 
        column_name, 
        data_type, 
        is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
    """

    df = pd.read_sql(query, conn)
    md = df.to_markdown(index=False)

    out_md.write_text(md)
    print(f"Wrote schema markdown to: {out_md}")

if __name__ == "__main__":
    main()
