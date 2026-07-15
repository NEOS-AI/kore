use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Key not found")]
    KeyNotFound,

    #[error("Invalid command: {0}")]
    InvalidCommand(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Entry too large")]
    EntryTooLarge,

    #[error("Out of memory")]
    OutOfMemory,

    #[error("Invalid protocol")]
    InvalidProtocol,

    #[error("Authentication required")]
    AuthRequired,

    #[error("Authentication failed")]
    AuthFailed,

    #[error("CAS mismatch")]
    CasMismatch,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Lock acquisition failed: {0}")]
    LockAcquisitionFailed(String),

    #[error("Lock extension failed: {0}")]
    LockExtensionFailed(String),

    #[error("Deadlock detected: {0}")]
    DeadlockDetected(String),

    #[error("Pub/Sub error: {0}")]
    PubSubError(String),

    #[error("Message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("Slow client timeout")]
    SlowClientTimeout,

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Comparison error: cannot compare NaN values")]
    ComparisonError,

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
}

impl Error {
    /// Format this error as a Redis RESP error payload (without the leading `-`).
    pub fn to_resp_string(&self) -> String {
        match self {
            // Redis emits WRONGTYPE / OOM without an ERR prefix.
            Error::WrongType => self.to_string(),
            Error::OutOfMemory => {
                "OOM command not allowed when used memory > 'maxmemory'".to_string()
            }
            other => format!("ERR {}", other),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
