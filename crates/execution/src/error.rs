/// Database persistence and data corruption errors.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Invalid direction in database: {0}")]
    InvalidDirection(#[from] crate::InvalidDirectionError),
    #[error("Invalid trade status in database: {0}")]
    InvalidTradeStatus(String),
    #[error("Invalid share quantity in database: {0}")]
    InvalidShareQuantity(i64),
    #[error("Invalid price cents in database: {0}")]
    InvalidPriceCents(i64),
    #[error("Execution missing ID after database save")]
    MissingExecutionId,
    #[error("Invalid symbol in database: {0}")]
    InvalidSymbol(String),
    #[error("Row not found for update: execution_id={execution_id}")]
    RowNotFound { execution_id: i64 },
    #[error("Execution not found: {0}")]
    ExecutionNotFound(i64),
    #[error("Execution error: {0}")]
    Execution(#[source] Box<crate::ExecutionError>),
}

impl From<crate::ExecutionError> for PersistenceError {
    fn from(err: crate::ExecutionError) -> Self {
        match err {
            crate::ExecutionError::Database(db_err) => Self::Database(db_err),
            other => Self::Execution(Box::new(other)),
        }
    }
}
