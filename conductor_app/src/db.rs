use rocket::{Build, Rocket};
use rocket_sync_db_pools::{database, postgres};

#[database("quest_db")]
pub struct QuestDbConn(postgres::Client);

pub async fn create_app_schema(rocket: Rocket<Build>) -> Rocket<Build> {
    log::info!("Creating application schema");
    QuestDbConn::get_one(&rocket)
        .await
        .expect("database mounted")
        .run(|conn| {
            log::info!("Creating producers table");
            conn.execute(
                r#"
            CREATE TABLE IF NOT EXISTS producers (name string, uuid string, schema string);"#,
                &[],
            )
        })
        .await
        .expect("cant init producers table");

    rocket
}
