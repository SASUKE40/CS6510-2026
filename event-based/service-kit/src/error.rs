//! The one error type every service returns. It renders as the contract's
//! JSON error body, and errors relayed from another service keep their code.
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Invalid,
    NotFound,
    Conflict,
    Unavailable,
    Internal,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Invalid => "INVALID",
            Kind::NotFound => "NOT_FOUND",
            Kind::Conflict => "CONFLICT",
            Kind::Unavailable => "UNAVAILABLE",
            Kind::Internal => "INTERNAL",
        }
    }
    pub fn parse(text: &str) -> Self {
        match text {
            "INVALID" => Kind::Invalid,
            "NOT_FOUND" => Kind::NotFound,
            "CONFLICT" => Kind::Conflict,
            "UNAVAILABLE" => Kind::Unavailable,
            _ => Kind::Internal,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Error {
    pub kind: Kind,
    pub code: String,
    pub message: String,
}
pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for Error {}

impl Error {
    pub fn new(kind: Kind, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind,
            code: code.into(),
            message: message.into(),
        }
    }
    pub fn bad(message: impl Into<String>) -> Self {
        Self::new(Kind::Invalid, "INVALID_REQUEST", message)
    }
    pub fn missing(message: &str) -> Self {
        Self::not_found("NOT_FOUND", message)
    }
    pub fn not_found(code: &str, message: &str) -> Self {
        Self::new(Kind::NotFound, code, message)
    }
    pub fn conflict(code: &str, message: &str) -> Self {
        Self::new(Kind::Conflict, code, message)
    }
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(Kind::Unavailable, "SERVICE_UNAVAILABLE", message)
    }
    pub fn internal(error: impl std::fmt::Display) -> Self {
        eprintln!("Internal error: {error}");
        Self::new(Kind::Internal, "INTERNAL_ERROR", "Internal server error")
    }
    /// The error's fields for a rejection event.
    pub fn to_event(&self) -> Value {
        json!({"error": self.code, "message": self.message, "kind": self.kind.as_str()})
    }
    /// The error carried by a rejection event.
    pub fn from_event(event: &Value) -> Self {
        let text = |key: &str| event[key].as_str().unwrap_or_default().to_owned();
        Self::new(Kind::parse(&text("kind")), text("error"), text("message"))
    }
    /// A business rejection, final however often the request is repeated.
    pub fn is_rejection(&self) -> bool {
        matches!(self.kind, Kind::Invalid | Kind::NotFound | Kind::Conflict)
    }
    pub fn status(&self) -> StatusCode {
        match self.kind {
            Kind::Invalid => StatusCode::BAD_REQUEST,
            Kind::NotFound => StatusCode::NOT_FOUND,
            Kind::Conflict => StatusCode::CONFLICT,
            Kind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Kind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Self::internal(error)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            self.status(),
            Json(json!({"error": self.code, "message": self.message})),
        )
            .into_response()
    }
}
