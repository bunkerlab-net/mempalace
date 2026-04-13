use turso::Connection;

use crate::error::Result;
use crate::palace::layers;

pub async fn run(connection: &Connection, wing: Option<&str>) -> Result<()> {
    let text = layers::wake_up(connection, wing).await?;
    println!("{text}");
    Ok(())
}
