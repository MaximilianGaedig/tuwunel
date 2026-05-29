use std::any::TypeId;

use ruma::{
	CanonicalJsonValue,
	api::{
		auth_scheme::{
			AccessToken, AccessTokenOptional, AppserviceToken, AppserviceTokenOptional,
			AuthScheme, NoAccessToken, NoAuthentication,
		},
		client::voip::get_turn_server_info,
		error::{ErrorKind, UnknownTokenErrorData},
		federation::{authentication::ServerSignatures, openid::get_openid_userinfo},
	},
};
use tuwunel_core::{Err, Error, Result};
use tuwunel_service::Services;

use super::{Auth, Request, Token, appservice::auth_appservice, server::auth_server};

/// Tag identifying an [`AuthScheme`] for tuwunel's purposes.
///
/// Ruma's `AuthScheme` is a trait, so endpoint-specific bypasses cannot be
/// expressed as enum match arms anymore. This tag is the value-side handle
/// used to route through `auth()` and to identify the unauthenticated case
/// inside `check_auth_still_required`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::router) enum Scheme {
	None,
	AccessToken,
	AccessTokenOptional,
	AppserviceToken,
	AppserviceTokenOptional,
	ServerSignatures,
}

/// Trait routing a concrete [`AuthScheme`] through the per-scheme dispatch.
///
/// `dispatch` is intentionally non-generic over the request type; the
/// caller passes `TypeId::of::<T>()` so each impl emits a single body
/// rather than monomorphizing per request.
pub(in crate::router) trait AuthDispatch: AuthScheme {
	const SCHEME: Scheme;

	fn dispatch(
		services: &Services,
		request: &mut Request,
		json_body: Option<&CanonicalJsonValue>,
		token: Token,
		route: TypeId,
	) -> impl Future<Output = Result<Auth>> + Send;
}

impl AuthDispatch for NoAccessToken {
	const SCHEME: Scheme = Scheme::None;

	async fn dispatch(
		services: &Services,
		request: &mut Request,
		json_body: Option<&CanonicalJsonValue>,
		token: Token,
		route: TypeId,
	) -> Result<Auth> {
		<NoAuthentication as AuthDispatch>::dispatch(services, request, json_body, token, route)
			.await
	}
}

impl AuthDispatch for NoAuthentication {
	const SCHEME: Scheme = Scheme::None;

	async fn dispatch(
		services: &Services,
		request: &mut Request,
		_json_body: Option<&CanonicalJsonValue>,
		token: Token,
		route: TypeId,
	) -> Result<Auth> {
		match token {
			| Token::Invalid
				if request.query.access_token.is_some()
					&& route == TypeId::of::<get_openid_userinfo::v1::Request>() =>
			{
				// OpenID federation endpoint uses a query param with the same name; drop
				// once query params for user auth are removed from the spec. Required to
				// make the integration manager work.
				Ok(Auth::default())
			},

			| Token::Invalid => unknown_token(),
			| Token::Expired(access_token) => expired_token(services, &access_token).await,

			| Token::User(user) => Ok(Auth {
				sender_user: Some(user.0),
				sender_device: Some(user.1),
				_expires_at: user.2,
				..Auth::default()
			}),

			| Token::Appservice(info) => Ok(Auth {
				appservice_info: Some(*info),
				..Auth::default()
			}),

			| Token::None => Ok(Auth::default()),
		}
	}
}

impl AuthDispatch for AccessToken {
	const SCHEME: Scheme = Scheme::AccessToken;

	async fn dispatch(
		services: &Services,
		request: &mut Request,
		_json_body: Option<&CanonicalJsonValue>,
		token: Token,
		route: TypeId,
	) -> Result<Auth> {
		match token {
			| Token::Invalid => unknown_token(),
			| Token::Expired(access_token) => expired_token(services, &access_token).await,
			| Token::Appservice(info) => Ok(auth_appservice(services, request, info).await?),
			| Token::User(user) => Ok(Auth {
				sender_user: Some(user.0),
				sender_device: Some(user.1),
				_expires_at: user.2,
				..Auth::default()
			}),
			| Token::None
				if route == TypeId::of::<get_turn_server_info::v3::Request>()
					&& services.server.config.turn_allow_guests =>
				Ok(Auth::default()),

			| Token::None => Err!(Request(MissingToken("Missing access token."))),
		}
	}
}

impl AuthDispatch for AccessTokenOptional {
	const SCHEME: Scheme = Scheme::AccessTokenOptional;

	async fn dispatch(
		services: &Services,
		_request: &mut Request,
		_json_body: Option<&CanonicalJsonValue>,
		token: Token,
		_route: TypeId,
	) -> Result<Auth> {
		match token {
			| Token::Invalid => unknown_token(),
			| Token::Expired(access_token) => expired_token(services, &access_token).await,
			| Token::User(user) => Ok(Auth {
				sender_user: Some(user.0),
				sender_device: Some(user.1),
				_expires_at: user.2,
				..Auth::default()
			}),
			| Token::Appservice(info) => Ok(Auth {
				appservice_info: Some(*info),
				..Auth::default()
			}),
			| Token::None => Ok(Auth::default()),
		}
	}
}

impl AuthDispatch for AppserviceToken {
	const SCHEME: Scheme = Scheme::AppserviceToken;

	async fn dispatch(
		services: &Services,
		_request: &mut Request,
		_json_body: Option<&CanonicalJsonValue>,
		token: Token,
		_route: TypeId,
	) -> Result<Auth> {
		match token {
			| Token::Invalid => unknown_token(),
			| Token::Expired(access_token) => expired_token(services, &access_token).await,
			| Token::User(_) =>
				Err!(Request(Unauthorized("Appservice tokens must be used on this endpoint."))),
			| Token::Appservice(info) => Ok(Auth {
				appservice_info: Some(*info),
				..Auth::default()
			}),
			| Token::None => Err!(Request(MissingToken("Missing access token."))),
		}
	}
}

impl AuthDispatch for AppserviceTokenOptional {
	const SCHEME: Scheme = Scheme::AppserviceTokenOptional;

	async fn dispatch(
		services: &Services,
		_request: &mut Request,
		_json_body: Option<&CanonicalJsonValue>,
		token: Token,
		_route: TypeId,
	) -> Result<Auth> {
		match token {
			| Token::Invalid => unknown_token(),
			| Token::Expired(access_token) => expired_token(services, &access_token).await,
			| Token::User(user) => Ok(Auth {
				sender_user: Some(user.0),
				sender_device: Some(user.1),
				_expires_at: user.2,
				..Auth::default()
			}),
			| Token::Appservice(info) => Ok(Auth {
				appservice_info: Some(*info),
				..Auth::default()
			}),
			| Token::None => Ok(Auth::default()),
		}
	}
}

impl AuthDispatch for ServerSignatures {
	const SCHEME: Scheme = Scheme::ServerSignatures;

	async fn dispatch(
		services: &Services,
		request: &mut Request,
		json_body: Option<&CanonicalJsonValue>,
		token: Token,
		_route: TypeId,
	) -> Result<Auth> {
		match token {
			| Token::Invalid => unknown_token(),
			| Token::Expired(access_token) => expired_token(services, &access_token).await,
			| Token::Appservice(_) | Token::User(_) =>
				Err!(Request(Unauthorized("Server signatures must be used on this endpoint."))),
			| Token::None => Ok(auth_server(services, request, json_body).await?),
		}
	}
}

fn unknown_token() -> Result<Auth> {
	Err(Error::BadRequest(
		ErrorKind::UnknownToken(UnknownTokenErrorData::new()),
		"Unknown access token.",
	))
}

async fn expired_token(services: &Services, access_token: &str) -> Result<Auth> {
	services
		.users
		.remove_access_token_value(access_token)
		.await;

	Err(Error::BadRequest(
		ErrorKind::UnknownToken(UnknownTokenErrorData { soft_logout: true }),
		"Expired access token.",
	))
}
