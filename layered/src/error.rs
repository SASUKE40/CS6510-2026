//! Application errors carry no HTTP or database-specific types.
#[derive(Debug)]
pub(crate) enum Kind {
    Invalid,
    NotFound,
    Conflict,
    Internal,
}
#[derive(Debug)]
pub(crate) struct Error {
    pub kind: Kind,
    pub code: &'static str,
    pub message: String,
}
pub(crate) type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn bad(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Invalid,
            code: "INVALID_REQUEST",
            message: message.into(),
        }
    }
    pub fn missing(message: &str) -> Self {
        Self::not_found("NOT_FOUND", message)
    }
    pub fn not_found(code: &'static str, message: &str) -> Self {
        Self {
            kind: Kind::NotFound,
            code,
            message: message.into(),
        }
    }
    pub fn conflict(code: &'static str, message: &str) -> Self {
        Self {
            kind: Kind::Conflict,
            code,
            message: message.into(),
        }
    }
    pub fn internal(error: impl std::fmt::Display) -> Self {
        eprintln!("Internal error: {error}");
        Self {
            kind: Kind::Internal,
            code: "INTERNAL_ERROR",
            message: "Internal server error".into(),
        }
    }
}
