use std::sync::Arc;

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, StrBytes};
use picomq_auth::{Audience, AuthError, AuthPrincipal, Authorizer};
use picomq_common::now_ms;
use picomq_metrics::Reason;

use crate::dispatch::RequestContext;
use crate::handlers::HandlerError;
use crate::handlers::common::{
    ILLEGAL_SASL_STATE, NO_ERROR, SASL_AUTHENTICATION_FAILED, UNSUPPORTED_SASL_MECHANISM,
    encode_response,
};
use crate::versions;

const PLAIN: &str = "PLAIN";

pub struct Session {
    authorizer: Arc<Authorizer>,
    state: State,
}

enum State {
    Negotiating,
    Authenticating { legacy: bool },
    Authenticated(AuthPrincipal),
}

pub struct Answer {
    pub frame: Option<Bytes>,
    pub close: bool,
}

pub fn is_sasl(api_key: i16) -> bool {
    api_key == ApiKey::SaslHandshake as i16 || api_key == ApiKey::SaslAuthenticate as i16
}

pub fn illegal_state(req: &RequestContext) -> Bytes {
    if req.api_key == ApiKey::SaslHandshake as i16 {
        handshake_response(req, ILLEGAL_SASL_STATE)
    } else {
        authenticate_response(req, ILLEGAL_SASL_STATE, Some("sasl is not enabled"))
    }
}

impl Session {
    pub fn new(authorizer: Arc<Authorizer>) -> Self {
        Self {
            authorizer,
            state: State::Negotiating,
        }
    }

    pub fn principal(&self) -> Option<&AuthPrincipal> {
        match &self.state {
            State::Authenticated(principal) => Some(principal),
            _ => None,
        }
    }

    pub async fn refresh(&mut self) -> Result<AuthPrincipal, AuthError> {
        let State::Authenticated(principal) = &mut self.state else {
            return Err(AuthError::Unauthenticated);
        };

        let fresh = self.authorizer.refresh(principal, now_ms()).await?;
        *principal = fresh.clone();
        Ok(fresh)
    }

    pub fn awaits_raw_token(&self) -> bool {
        matches!(self.state, State::Authenticating { legacy: true })
    }

    pub fn handshake(&mut self, req: &RequestContext, body: &[u8]) -> Result<Answer, HandlerError> {
        supported(req)?;
        let mut body = Bytes::copy_from_slice(body);
        let request = SaslHandshakeRequest::decode(&mut body, req.api_version)
            .map_err(|error| HandlerError::Protocol(error.to_string()))?;

        let (error_code, close) = match self.state {
            State::Negotiating if request.mechanism.as_str() == PLAIN => {
                self.state = State::Authenticating {
                    legacy: req.api_version == 0,
                };
                (NO_ERROR, false)
            }
            State::Negotiating => (UNSUPPORTED_SASL_MECHANISM, true),
            State::Authenticating { .. } => (ILLEGAL_SASL_STATE, true),
            State::Authenticated(_) => (ILLEGAL_SASL_STATE, false),
        };

        Ok(Answer {
            frame: Some(handshake_response(req, error_code)),
            close,
        })
    }

    pub async fn authenticate(
        &mut self,
        req: &RequestContext,
        body: &[u8],
    ) -> Result<Answer, HandlerError> {
        supported(req)?;
        let mut body = Bytes::copy_from_slice(body);
        let request = SaslAuthenticateRequest::decode(&mut body, req.api_version)
            .map_err(|error| HandlerError::Protocol(error.to_string()))?;

        match self.state {
            State::Authenticating { legacy: false } => {}
            State::Negotiating | State::Authenticating { legacy: true } => {
                return Ok(Answer {
                    frame: None,
                    close: true,
                });
            }
            State::Authenticated(_) => {
                return Ok(Answer {
                    frame: Some(authenticate_response(
                        req,
                        ILLEGAL_SASL_STATE,
                        Some("already authenticated"),
                    )),
                    close: false,
                });
            }
        }

        Ok(match verify(&self.authorizer, &request.auth_bytes).await {
            Ok(principal) => {
                self.state = State::Authenticated(principal);
                Answer {
                    frame: Some(authenticate_response(req, NO_ERROR, None)),
                    close: false,
                }
            }
            Err(message) => Answer {
                frame: Some(authenticate_response(
                    req,
                    SASL_AUTHENTICATION_FAILED,
                    Some(message),
                )),
                close: true,
            },
        })
    }

    pub async fn authenticate_raw(&mut self, token: &[u8]) -> Answer {
        match verify(&self.authorizer, token).await {
            Ok(principal) => {
                self.state = State::Authenticated(principal);
                Answer {
                    frame: Some(Bytes::new()),
                    close: false,
                }
            }
            Err(_) => Answer {
                frame: None,
                close: true,
            },
        }
    }
}

fn supported(req: &RequestContext) -> Result<(), HandlerError> {
    if versions::is_supported(req.api_key, req.api_version) {
        return Ok(());
    }
    Err(HandlerError::Protocol(format!(
        "unsupported api key {} version {}",
        req.api_key, req.api_version
    )))
}

async fn verify(authorizer: &Authorizer, token: &[u8]) -> Result<AuthPrincipal, &'static str> {
    let (username, password) =
        plain(token).ok_or_else(|| auth_failure(Reason::Malformed, "malformed PLAIN token"))?;

    let principal = authorizer
        .authenticate(password, Audience::Kafka, now_ms())
        .await
        .map_err(|err| match err {
            AuthError::Store(_) => "invalid username or password",
            _ => auth_failure(Reason::Credential, "invalid username or password"),
        })?;
    if principal.id != username {
        return Err(auth_failure(
            Reason::Credential,
            "invalid username or password",
        ));
    }
    if principal.scope.auto_prefix {
        return Err(auth_failure(
            Reason::AutoPrefix,
            "auto prefix scopes are not supported over Kafka",
        ));
    }

    Ok(principal)
}

fn auth_failure(reason: Reason, message: &'static str) -> &'static str {
    picomq_metrics::auth_failure(Audience::Kafka, reason);
    message
}

fn plain(token: &[u8]) -> Option<(&str, &str)> {
    let mut parts = token.split(|byte| *byte == 0);
    parts.next()?;
    let username = std::str::from_utf8(parts.next()?).ok()?;
    let password = std::str::from_utf8(parts.next()?).ok()?;

    parts.next().is_none().then_some((username, password))
}

fn handshake_response(req: &RequestContext, error_code: i16) -> Bytes {
    let mechanisms = if error_code == ILLEGAL_SASL_STATE {
        Vec::new()
    } else {
        vec![StrBytes::from_static_str(PLAIN)]
    };
    let response = SaslHandshakeResponse::default()
        .with_error_code(error_code)
        .with_mechanisms(mechanisms);
    encode_response(req.correlation_id, req.api_version, &response).0
}

fn authenticate_response(req: &RequestContext, error_code: i16, message: Option<&str>) -> Bytes {
    let response = SaslAuthenticateResponse::default()
        .with_error_code(error_code)
        .with_error_message(message.map(|message| StrBytes::from(message.to_owned())));
    encode_response(req.correlation_id, req.api_version, &response).0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_tokens_carry_an_authorization_id_a_username_and_a_password() {
        assert_eq!(
            plain(b"\0svc/ingest\0secret"),
            Some(("svc/ingest", "secret"))
        );
        assert_eq!(
            plain(b"admin\0svc/ingest\0secret"),
            Some(("svc/ingest", "secret"))
        );
        assert_eq!(plain(b"svc/ingest\0secret"), None);
        assert_eq!(plain(b"\0svc/ingest\0secret\0extra"), None);
        assert_eq!(plain(b"\0\xff\0secret"), None);
    }
}
