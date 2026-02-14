use crate::{FractionalShares, Positive};

/// Database persistence and data corruption errors.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Invalid direction in database: {0}")]
    InvalidDirection(#[from] crate::InvalidDirectionError),
    #[error("invalid trade status in database: {status_provided}")]
    InvalidTradeStatus { status_provided: String },
    #[error("Invalid share quantity in database: {0}")]
    InvalidShareQuantity(f64),
    #[error("Share quantity {0} cannot be converted to f64")]
    ShareQuantityConversionFailed(Positive<FractionalShares>),
    #[error("Invalid price cents in database: {0}")]
    InvalidPriceCents(i64),
    #[error("Execution missing ID after database save")]
    MissingExecutionId,
    #[error("invalid symbol in database: {symbol_provided}")]
    InvalidSymbol { symbol_provided: String },
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
