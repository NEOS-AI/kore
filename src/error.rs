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
}

pub type Result<T> = std::result::Result<T, Error>;
