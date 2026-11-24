//! Explicit client access to a discovered Events catalog, polls, and subscriptions.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{
    EXTENSION, LIST, POLL,
    wire::{DeliveryMode, EventsError, ListEventsResult, PollParams, PollResult},
};
use crate::{
    Peer, RoleClient, ServiceError,
    model::{ClientRequest, CustomRequest, ProtocolVersion, ServerResult},
};

mod subscription;
#[cfg(feature = "events-webhook-receiver")]
mod webhook;
pub use subscription::{EventSubscription, EventUpdate, SubscriptionOptions};

/// A client for a peer that advertised the Events Beta via modern discovery.
///
/// Start the underlying service in Discover mode (or Auto with successful
/// discovery). Invoking these methods never opts a legacy peer into Events.
pub struct EventsClient {
    peer: Peer<RoleClient>,
    catalog: Arc<tokio::sync::Mutex<Catalog>>,
}

#[derive(Clone)]
struct CatalogEntry {
    delivery: Vec<DeliveryMode>,
    #[cfg(feature = "transport-streamable-http-client")]
    input_schema: Arc<crate::model::JsonObject>,
}

#[derive(Default)]
struct Catalog {
    routes: Option<HashMap<String, CatalogEntry>>,
    missing: HashSet<String>,
}

impl std::fmt::Debug for EventsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventsClient").finish_non_exhaustive()
    }
}

/// A validated catalog entry selected for this request; the HTTP worker
/// consumes it without maintaining its own Events catalog.
#[cfg(feature = "transport-streamable-http-client")]
#[derive(Clone)]
pub(crate) struct RoutingSchema(pub(crate) Option<Arc<crate::model::JsonObject>>);

impl EventsClient {
    /// Use a peer whose discovery result advertises Events.
    pub fn new(peer: Peer<RoleClient>) -> Self {
        Self {
            peer,
            catalog: Arc::default(),
        }
    }

    /// List one catalog page. If pagination becomes stale, restart at `None`.
    /// Relisting from the first page clears routing schemas and remembered misses.
    pub async fn list_page(&self, cursor: Option<&str>) -> Result<ListEventsResult, EventsError> {
        self.require_advertisement()?;
        if cursor.is_none() {
            self.invalidate_catalog().await;
        }
        self.request_page(cursor).await
    }

    async fn request_page(&self, cursor: Option<&str>) -> Result<ListEventsResult, EventsError> {
        let params = cursor.map(|cursor| json!({ "cursor": cursor }));
        let request = CustomRequest::new(LIST, params);
        #[cfg(feature = "transport-streamable-http-client")]
        let request = {
            let mut request = request;
            request.extensions.insert(RoutingSchema(None));
            request
        };
        let page: ListEventsResult = self.request(request).await?;
        #[cfg(feature = "transport-streamable-http-client")]
        let page = filter_annotations(page);
        let mut names = HashSet::new();
        if !page.events.iter().all(|event| names.insert(&event.name)) {
            return Err(EventsError::UnexpectedResponse);
        }
        Ok(page)
    }

    /// Poll once, without scheduling, buffering, or advancing the caller's
    /// checkpoint. Persist the result cursor only after consuming the batch.
    /// A null cursor starts at the current head; never persist null for replay.
    /// A timestamp the SDK cannot represent fails the whole page with
    /// [`EventsError::UnexpectedResponse`], without returning a checkpoint.
    ///
    /// With Streamable HTTP support compiled, the first poll lists the
    /// catalog to resolve routing schemas. A missing name refreshes it once
    /// and is remembered until an explicit relist or a header-mismatch retry.
    pub async fn poll(&self, params: PollParams) -> Result<PollResult, EventsError> {
        self.require_advertisement()?;
        #[cfg(feature = "transport-streamable-http-client")]
        {
            let name = params.name.clone();
            let schema = self.schema(&name, false).await?;
            let request = routed_poll(&params, schema)?;
            let result = self.request(request).await;
            if matches!(&result, Err(EventsError::HeaderMismatch(_))) {
                let schema = self.schema(&name, true).await?;
                let request = routed_poll(&params, schema)?;
                return self.request(request).await;
            }
            result
        }
        #[cfg(not(feature = "transport-streamable-http-client"))]
        self.request(poll_request(&params)?).await
    }

    fn require_advertisement(&self) -> Result<(), EventsError> {
        if self.peer.used_discovery()
            && self.peer.peer_info().is_some_and(|peer| {
                peer.protocol_version >= ProtocolVersion::V_2026_07_28
                    && peer
                        .capabilities
                        .extensions
                        .as_ref()
                        .is_some_and(|extensions| extensions.contains_key(EXTENSION))
            })
        {
            Ok(())
        } else {
            Err(EventsError::MethodNotFound)
        }
    }

    async fn request<T: DeserializeOwned>(&self, request: CustomRequest) -> Result<T, EventsError> {
        let result = self
            .peer
            .send_request(ClientRequest::CustomRequest(request))
            .await
            .map_err(from_service)?;
        decode_result(result)
    }

    #[cfg(feature = "transport-streamable-http-client")]
    async fn schema(
        &self,
        name: &str,
        refresh: bool,
    ) -> Result<Arc<crate::model::JsonObject>, EventsError> {
        Ok(self.descriptor_cached(name, refresh).await?.input_schema)
    }

    async fn invalidate_catalog(&self) {
        *self.catalog.lock().await = Catalog::default();
    }

    async fn descriptor_cached(
        &self,
        name: &str,
        refresh: bool,
    ) -> Result<CatalogEntry, EventsError> {
        let mut cached = self.catalog.lock().await;
        if refresh {
            *cached = Catalog::default();
        }
        if !cached.missing.contains(name) {
            if let Some(entry) = cached.routes.as_ref().and_then(|routes| routes.get(name)) {
                return Ok(entry.clone());
            }
            // Clear stale routing on a failed refresh too.
            cached.routes = None;
            let catalog = self.load_catalog().await?;
            let entry = catalog.get(name).cloned();
            if entry.is_none() {
                cached.missing.insert(name.to_owned());
            }
            cached.routes = Some(catalog);
            if let Some(entry) = entry {
                return Ok(entry);
            }
        }
        Err(EventsError::NotFound {
            kind: Some("event".into()),
        })
    }

    async fn load_catalog(&self) -> Result<HashMap<String, CatalogEntry>, EventsError> {
        match self.drain_pages().await {
            Err(EventsError::InvalidParams(_)) => self.drain_pages().await,
            result => result,
        }
    }

    async fn drain_pages(&self) -> Result<HashMap<String, CatalogEntry>, EventsError> {
        let mut catalog = HashMap::new();
        let mut seen = HashSet::new();
        let mut cursor = None;
        loop {
            let page = self.request_page(cursor.as_deref()).await?;
            for descriptor in page.events {
                if catalog
                    .insert(
                        descriptor.name,
                        CatalogEntry {
                            delivery: descriptor.delivery,
                            #[cfg(feature = "transport-streamable-http-client")]
                            input_schema: descriptor.input_schema,
                        },
                    )
                    .is_some()
                {
                    return Err(EventsError::UnexpectedResponse);
                }
            }
            let Some(next) = page.next_cursor else {
                return Ok(catalog);
            };
            if !seen.insert(next.clone()) {
                return Err(EventsError::UnexpectedResponse);
            }
            cursor = Some(next);
        }
    }
}

fn from_service(error: ServiceError) -> EventsError {
    match error {
        ServiceError::McpError(error) => EventsError::from(error),
        error => EventsError::Service(Box::new(error)),
    }
}

fn decode_result<T: DeserializeOwned>(result: ServerResult) -> Result<T, EventsError> {
    let ServerResult::CustomResult(result) = result else {
        return Err(EventsError::UnexpectedResponse);
    };
    serde_json::from_value(result.0).map_err(|_| EventsError::UnexpectedResponse)
}

fn poll_request(params: &impl Serialize) -> Result<CustomRequest, EventsError> {
    let params: Value = serde_json::to_value(params)
        .map_err(|_| EventsError::InvalidParams("invalid poll parameters".into()))?;
    Ok(CustomRequest::new(POLL, Some(params)))
}

#[cfg(feature = "transport-streamable-http-client")]
fn routed_poll(
    params: &PollParams,
    schema: Arc<crate::model::JsonObject>,
) -> Result<CustomRequest, EventsError> {
    route(poll_request(params)?, params.arguments.as_ref(), schema)
}

#[cfg(feature = "transport-streamable-http-client")]
fn route(
    mut request: CustomRequest,
    arguments: Option<&crate::model::JsonObject>,
    schema: Arc<crate::model::JsonObject>,
) -> Result<CustomRequest, EventsError> {
    crate::transport::common::mcp_headers::validate_event_header_arguments(arguments, &schema)
        .map_err(EventsError::InvalidParams)?;
    request.extensions.insert(RoutingSchema(Some(schema)));
    Ok(request)
}

#[cfg(feature = "transport-streamable-http-client")]
fn filter_annotations(mut page: ListEventsResult) -> ListEventsResult {
    page.events.retain(|descriptor| {
        if let Err(reason) = crate::transport::common::mcp_headers::validate_param_header_annotations(&descriptor.input_schema) {
            tracing::warn!(event = %descriptor.name, "rejecting invalid x-mcp-header annotations: {reason}");
            false
        } else {
            true
        }
    });
    page
}

#[cfg(all(test, feature = "transport-streamable-http-client"))]
mod tests {
    use super::*;

    #[test]
    fn malformed_entries_are_excluded_before_their_headers_are_used() {
        let event = |name, field, annotation| {
            json!({
                "name": name, "delivery": ["poll"], "payloadSchema": {},
                "inputSchema": {"properties": {"scope": {"type": field, "x-mcp-header": annotation}}}
            })
        };
        let page = serde_json::from_value(json!({
            "resultType": "complete", "nextCursor": "next",
            "events": [
                event("valid", "string", "Scope"),
                event("invalid-token", "string", "bad token"),
                event("invalid-schema", "number", "Scope"),
                {"name":"missing-payload","delivery":["poll"],"inputSchema":{}}
            ]
        }))
        .unwrap();
        let result = filter_annotations(page);
        assert_eq!(result.events.len(), 1);
        assert_eq!(result.events[0].name, "valid");
        assert_eq!(result.next_cursor.as_deref(), Some("next"));
    }
}
