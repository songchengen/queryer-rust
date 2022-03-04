use std::io::Cursor;

use anyhow::Result;
use polars::{
    io::SerReader,
    prelude::{ChunkCompare, CsvReader},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
    let data = reqwest::get(url).await?.text().await?;

    let df = CsvReader::new(Cursor::new(data))
        .infer_schema(Some(16))
        .finish()?;

    let filtered = df.filter(&df["new_deaths"].gt(50))?;

    println!("{:?}", filtered.select(("location", "new_deaths")));

    Ok(())
}
