use rocket_sync_db_pools::{database, postgres};

#[database("quest_db")]
pub struct QuestDbConn(postgres::Client);
