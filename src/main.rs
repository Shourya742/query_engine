use anyhow::Result;
use query_engine::db::Database;

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::new_on_csv();
    let table_name = "employee".to_string();
    let filepath = "./tests/csv/sample.csv".to_string();
    db.create_csv_table(table_name, filepath)?;
    let output = db
        .run("select first_name from employee where last_name = 'Hopkins'")
        .await?;
    println!("Output: {output:?}");

    Ok(())
}
