use std::{
    collections::VecDeque,
    fmt,
    hash::{BuildHasher, RandomState},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Stream, stream};
use serde::Deserialize;
use tokio::{
    sync::mpsc,
    time::{Instant, sleep_until, timeout_at},
};

use super::*;
use crate::{
    events::{
        ACTIVE, ERROR, EVENT, HEARTBEAT, STREAM, StreamParams, TERMINATED, wire::EventOccurrence,
    },
    model::{
        ErrorCode, GetMeta, JsonObject, RequestId, ServerNotification, SubscriptionsListenResult,
    },
    service::{PeerRequestOptions, RequestHandle},
};

const FRAME_CAPACITY: usize = 64;
const DEDUP_CAPACITY: usize = 1_024;
// Leave time for the SDK server's 10-second activation budget and its reply.
const ACTIVATION_BUDGET: Duration = Duration::from_secs(15);
const POLL_BUDGET: Duration = Duration::from_secs(15);
const POLL_FLOOR_MS: u64 = 1_000;
const POLL_BURST: u8 = 10;

/// Mode selection and replay position for [`EventsClient::subscribe`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct SubscriptionOptions {
    /// Require a particular delivery mode. By default, prefer push to polling.
    pub mode: Option<DeliveryMode>,
    /// Last consumed Events cursor, or `None` to start at the current head.
    pub cursor: Option<String>,
    /// Limit historical replay by age, in milliseconds.
    pub max_age_ms: Option<u64>,
}

/// An ordered update from one Events subscription.
#[derive(Debug)]
#[non_exhaustive]
pub enum EventUpdate {
    /// A delivered occurrence. Duplicate IDs within the recent window are suppressed.
    Occurrence(EventOccurrence),
    /// A safe position to persist, including after quiet progress.
    Checkpoint(String),
    /// Some occurrences may have been skipped. Delivered before the new checkpoint.
    Gap,
    /// Delivery can continue, possibly after reconnecting.
    RecoverableError(EventsError),
    /// Delivery ended. A successful final frame is optional; an explicit
    /// termination notification alone suffices. Global transport shutdown
    /// need not send a final frame.
    Terminated {
        /// Why delivery ended.
        error: EventsError,
        /// Last consumed replay position, if known.
        cursor: Option<String>,
    },
}

#[cfg(not(feature = "local"))]
type Updates = Pin<Box<dyn Stream<Item = EventUpdate> + Send>>;
#[cfg(feature = "local")]
type Updates = Pin<Box<dyn Stream<Item = EventUpdate>>>;

/// One push or managed-poll subscription. Poll the stream to drive delivery.
///
/// A checkpoint follows the events it covers; only persist it after handling
/// those events. Dropping this handle stops polling or cancels the push request.
pub struct EventSubscription {
    updates: Option<Updates>,
}

impl fmt::Debug for EventSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSubscription").finish_non_exhaustive()
    }
}

impl Stream for EventSubscription {
    type Item = EventUpdate;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(updates) = &mut self.updates else {
            return Poll::Ready(None);
        };
        let result = updates.as_mut().poll_next(cx);
        if matches!(result, Poll::Ready(None)) {
            self.updates = None;
        }
        result
    }
}

impl EventsClient {
    /// Select a usable catalog mode and create a subscription to one event type.
    ///
    /// The delivery request starts when the returned stream is polled. Drop
    /// the stream to stop delivery without affecting other subscriptions.
    pub async fn subscribe(
        &self,
        name: impl Into<String>,
        arguments: Option<JsonObject>,
        options: SubscriptionOptions,
    ) -> Result<EventSubscription, EventsError> {
        self.require_advertisement()?;
        let name = name.into();
        let descriptor = self.descriptor_cached(&name, false).await?;
        let mode = match options.mode {
            Some(mode) if supports(&descriptor, mode) => mode,
            Some(mode) => return Err(unsupported(mode)),
            None if supports(&descriptor, DeliveryMode::Push) => DeliveryMode::Push,
            None if supports(&descriptor, DeliveryMode::Poll) => DeliveryMode::Poll,
            None => return Err(unsupported(DeliveryMode::Push)),
        };
        let driver = Driver {
            client: EventsClient {
                peer: self.peer.clone(),
                catalog: self.catalog.clone(),
            },
            name,
            descriptor,
            arguments,
            mode,
            cursor: options.cursor,
            max_age_ms: options.max_age_ms,
            delivery: Delivery::default(),
            push: None,
            burst: 0,
            retry: 0,
            header_retried: false,
            next_at: None,
            done: false,
        };
        Ok(EventSubscription {
            updates: Some(Box::pin(stream::unfold(driver, |mut driver| async {
                let item = driver.next().await?;
                Some((item, driver))
            }))),
        })
    }
}

fn supports(descriptor: &CatalogEntry, mode: DeliveryMode) -> bool {
    matches!(mode, DeliveryMode::Push | DeliveryMode::Poll) && descriptor.delivery.contains(&mode)
}

fn unsupported(mode: DeliveryMode) -> EventsError {
    EventsError::Unsupported {
        feature: "deliveryMode".into(),
        value: Some(json!(mode)),
        reason: None,
    }
}

// Keep only the current poll page (or one push frame) outside the existing
// bounded notification channel. Advance over it as the consumer pulls.
#[derive(Default)]
struct Delivery {
    events: std::vec::IntoIter<EventOccurrence>,
    event: Option<EventOccurrence>,
    position: Option<(Option<String>, bool)>,
    gap: bool,
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl Delivery {
    fn stage_position(&mut self, position: Option<String>, truncated: bool) {
        self.gap = truncated && position.is_some();
        self.position = Some((position, self.gap));
    }

    fn next(&mut self, cursor: &mut Option<String>) -> Option<EventUpdate> {
        if std::mem::take(&mut self.gap) {
            return Some(EventUpdate::Gap);
        }
        for event in self.event.take().into_iter().chain(self.events.by_ref()) {
            if self.seen.insert(event.event_id.clone()) {
                if self.order.len() == DEDUP_CAPACITY
                    && let Some(old) = self.order.pop_front()
                {
                    self.seen.remove(&old);
                }
                self.order.push_back(event.event_id.clone());
                return Some(EventUpdate::Occurrence(event));
            }
        }
        if let Some((position, report_gap_checkpoint)) = self.position.take()
            && (*cursor != position || report_gap_checkpoint)
        {
            *cursor = position;
            return cursor.clone().map(EventUpdate::Checkpoint);
        }
        None
    }
}

struct Driver {
    client: EventsClient,
    name: String,
    descriptor: CatalogEntry,
    arguments: Option<JsonObject>,
    mode: DeliveryMode,
    cursor: Option<String>,
    max_age_ms: Option<u64>,
    delivery: Delivery,
    push: Option<Push>,
    burst: u8,
    retry: u8,
    header_retried: bool,
    next_at: Option<Instant>,
    done: bool,
}

impl Driver {
    async fn next(&mut self) -> Option<EventUpdate> {
        while !self.done {
            if let Some(update) = self.delivery.next(&mut self.cursor) {
                return Some(update);
            }
            if let Some(at) = self.next_at.take() {
                sleep_until(at).await;
            }
            let result = match self.mode {
                DeliveryMode::Push => self.push_update().await,
                DeliveryMode::Poll => self.poll_page().await.map(|()| None),
                _ => unreachable!("mode selected from usable catalog delivery"),
            };
            match result {
                Ok(Some(update)) => return Some(update),
                Ok(None) => {}
                Err(EventsError::HeaderMismatch(_)) if !self.header_retried => {
                    self.push = None;
                    self.header_retried = true;
                    let refreshed = self.client.descriptor_cached(&self.name, true).await;
                    match refreshed {
                        Ok(descriptor) if supports(&descriptor, self.mode) => {
                            self.descriptor = descriptor
                        }
                        Ok(_) => return Some(self.finish(unsupported(self.mode)).await),
                        Err(error) => return Some(self.finish(error).await),
                    }
                }
                Err(error) if self.can_retry(&error) => {
                    self.push = None;
                    self.burst = 0;
                    let minimum = if self.mode == DeliveryMode::Poll {
                        POLL_FLOOR_MS
                    } else {
                        0
                    };
                    self.next_at = Some(Instant::now() + retry_delay(self.retry, minimum));
                    self.retry = self.retry.saturating_add(1);
                    return Some(EventUpdate::RecoverableError(error));
                }
                Err(error) => return Some(self.finish(error).await),
            }
        }
        None
    }

    fn can_retry(&self, error: &EventsError) -> bool {
        !self.client.peer.is_transport_closed() && retryable(error)
    }

    async fn finish(&mut self, error: EventsError) -> EventUpdate {
        self.done = true;
        self.push = None;
        if matches!(&error, EventsError::NotFound { .. })
            || matches!(&error, EventsError::Unsupported { reason: Some(reason), .. } if reason == "schema_changed")
        {
            self.client.invalidate_catalog().await;
        }
        EventUpdate::Terminated {
            error,
            cursor: self.cursor.clone(),
        }
    }

    async fn poll_page(&mut self) -> Result<(), EventsError> {
        let params = PollParams {
            name: self.name.clone(),
            arguments: self.arguments.clone(),
            cursor: self.cursor.clone(),
            max_age_ms: self.max_age_ms,
            max_events: None,
        };
        let request = self.routed(poll_request(&params)?)?;
        let result = timeout_at(Instant::now() + POLL_BUDGET, async {
            let request = self
                .client
                .peer
                .send_cancellable_request(
                    ClientRequest::CustomRequest(request),
                    PeerRequestOptions::no_options(),
                )
                .await?;
            Request::new(request).result().await
        })
        .await
        .map_err(|_| timed_out(POLL_BUDGET))?
        .map_err(from_service)?;
        let ServerResult::CustomResult(result) = result else {
            return Err(EventsError::UnexpectedResponse);
        };
        let result: PollResult = parse(result.0)?;
        if result.events.iter().any(|event| event.name != self.name) {
            return Err(EventsError::UnexpectedResponse);
        }
        self.header_retried = false;
        self.max_age_ms = None;
        self.retry = 0;
        let progressed = result.cursor.is_some() && self.cursor != result.cursor;
        if result.has_more && progressed {
            drain_page(&mut self.burst).await;
        } else {
            self.burst = 0;
            let delay = if result.has_more {
                POLL_FLOOR_MS
            } else {
                result.next_poll_ms.max(POLL_FLOOR_MS)
            };
            self.next_at = Some(deadline(delay)?);
        }
        self.delivery.events = result.events.into_iter();
        self.delivery
            .stage_position(result.cursor, result.truncated);
        Ok(())
    }

    fn routed(&self, request: CustomRequest) -> Result<CustomRequest, EventsError> {
        #[cfg(feature = "transport-streamable-http-client")]
        return route(
            request,
            self.arguments.as_ref(),
            self.descriptor.input_schema.clone(),
        );
        #[cfg(not(feature = "transport-streamable-http-client"))]
        Ok(request)
    }

    async fn open(&self) -> Result<Push, EventsError> {
        let params = StreamParams {
            name: self.name.clone(),
            arguments: self.arguments.clone(),
            cursor: self.cursor.clone(),
            max_age_ms: self.max_age_ms,
            heartbeat_ms: None,
        };
        let value = serde_json::to_value(params).map_err(|_| EventsError::UnexpectedResponse)?;
        let request = self.routed(CustomRequest::new(STREAM, Some(value)))?;
        let deadline = Instant::now() + ACTIVATION_BUDGET;
        let (request, frames) = timeout_at(
            deadline,
            self.client.peer.send_subscription_request(
                ClientRequest::CustomRequest(request),
                PeerRequestOptions::no_options(),
                FRAME_CAPACITY,
            ),
        )
        .await
        .map_err(|_| timed_out(ACTIVATION_BUDGET))?
        .map_err(from_service)?;
        Ok(Push {
            id: request.id.clone(),
            request: Request::new(request),
            frames,
            deadline,
            watchdog: ACTIVATION_BUDGET,
            active: false,
        })
    }

    async fn push_update(&mut self) -> Result<Option<EventUpdate>, EventsError> {
        if self.push.is_none() {
            self.push = Some(self.open().await?);
        }
        let push = self.push.as_mut().expect("opened push request");
        let frame = push.next().await?;
        if push.active && !matches!(&frame, Frame::Active(_) | Frame::Unknown) {
            self.retry = 0;
        }
        match frame {
            Frame::Unknown => {}
            Frame::Active(active) => {
                if active.heartbeat_ms == 0 {
                    return Err(EventsError::UnexpectedResponse);
                }
                push.active = true;
                push.watchdog = Duration::from_millis(active.heartbeat_ms.saturating_mul(3));
                push.reset_watchdog()?;
                self.header_retried = false;
                self.delivery
                    .stage_position(active.position.cursor, active.truncated);
            }
            _ if !push.active => return Err(EventsError::UnexpectedResponse),
            Frame::Event(event) => {
                if event.name != self.name || event.event_id.is_empty() {
                    return Err(EventsError::UnexpectedResponse);
                }
                let cursor = event.cursor.clone();
                self.delivery.event = Some(event);
                self.delivery.stage_position(cursor, false);
            }
            Frame::Heartbeat(position) => self.delivery.stage_position(position.cursor, false),
            Frame::Error(error) => return Ok(Some(EventUpdate::RecoverableError(error))),
            Frame::Terminated(error) => {
                // Release the pending request even if the peer never writes
                // the final frame. The notification alone ends the subscription.
                return Ok(Some(self.finish(error).await));
            }
        }
        Ok(None)
    }
}

struct Request(Option<RequestHandle<RoleClient>>);

impl Request {
    fn new(request: RequestHandle<RoleClient>) -> Self {
        Self(Some(request))
    }

    fn handle(&mut self) -> Result<&mut RequestHandle<RoleClient>, ServiceError> {
        self.0.as_mut().ok_or(ServiceError::UnexpectedResponse)
    }

    async fn result(&mut self) -> Result<ServerResult, ServiceError> {
        let result = (&mut self.handle()?.rx).await;
        self.disarm();
        result.map_err(|_| ServiceError::TransportClosed)?
    }

    fn disarm(&mut self) {
        if let Some(request) = self.0.take() {
            request.peer.unregister_subscription(&request.id);
        }
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        if let Some(mut request) = self.0.take() {
            request.peer.unregister_subscription(&request.id);
            if matches!(
                request.rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ) {
                request
                    .peer
                    .try_cancel_request(request.id, Some("Events request cancelled".into()));
            }
        }
    }
}

struct Push {
    id: RequestId,
    request: Request,
    frames: mpsc::Receiver<ServerNotification>,
    deadline: Instant,
    watchdog: Duration,
    active: bool,
}

impl Push {
    async fn next(&mut self) -> Result<Frame, EventsError> {
        tokio::select! {
            biased;
            Some(frame) = self.frames.recv() => {
                if frame.get_meta().subscription_id().as_ref() != Some(&self.id) {
                    return Err(EventsError::UnexpectedResponse);
                }
                if self.active {
                    self.reset_watchdog()?;
                }
                Frame::decode(frame)
            }
            result = self.request.result() => {
                let complete = match result.map_err(from_service)? {
                    ServerResult::SubscriptionsListenResult(result) => result,
                    ServerResult::CustomResult(result)
                        if result.0.as_object().is_some_and(|object| object.len() == 2) =>
                    {
                        parse::<SubscriptionsListenResult>(result.0)?
                    }
                    _ => return Err(EventsError::UnexpectedResponse),
                };
                if !complete.result_type.is_complete() || complete.meta.subscription_id().as_ref() != Some(&self.id) {
                    return Err(EventsError::UnexpectedResponse);
                }
                Err(from_service(ServiceError::TransportClosed))
            }
            () = sleep_until(self.deadline) => Err(timed_out(self.watchdog)),
        }
    }

    fn reset_watchdog(&mut self) -> Result<(), EventsError> {
        self.deadline = Instant::now()
            .checked_add(self.watchdog)
            .ok_or(EventsError::UnexpectedResponse)?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct Position {
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Active {
    #[serde(flatten)]
    position: Position,
    #[serde(default)]
    truncated: bool,
    heartbeat_ms: u64,
}

#[derive(Deserialize)]
struct Problem {
    error: crate::ErrorData,
}

enum Frame {
    Active(Active),
    Event(EventOccurrence),
    Heartbeat(Position),
    Error(EventsError),
    Terminated(EventsError),
    Unknown,
}

impl Frame {
    fn decode(notification: ServerNotification) -> Result<Self, EventsError> {
        let ServerNotification::CustomNotification(notification) = notification else {
            return Err(EventsError::UnexpectedResponse);
        };
        if notification.method.starts_with("notifications/events/")
            && !matches!(
                notification.method.as_str(),
                ACTIVE | EVENT | HEARTBEAT | ERROR | TERMINATED
            )
        {
            return Ok(Self::Unknown);
        }
        let value = notification.params.ok_or(EventsError::UnexpectedResponse)?;
        Ok(match notification.method.as_str() {
            ACTIVE => Self::Active(parse(value)?),
            EVENT => {
                let mut event: EventOccurrence = parse(value)?;
                let metadata = notification
                    .extensions
                    .get::<crate::model::NotificationMetaObject>();
                if let Some(metadata) = metadata {
                    event
                        .meta
                        .get_or_insert_default()
                        .extend(metadata.0.0.clone());
                }
                if let Some(meta) = &mut event.meta {
                    meta.remove("io.modelcontextprotocol/subscriptionId");
                    if meta.is_empty() {
                        event.meta = None;
                    }
                }
                Self::Event(event)
            }
            HEARTBEAT => Self::Heartbeat(parse(value)?),
            ERROR => Self::Error(parse::<Problem>(value)?.error.into()),
            TERMINATED => Self::Terminated(parse::<Problem>(value)?.error.into()),
            _ => return Err(EventsError::UnexpectedResponse),
        })
    }
}

fn parse<T: DeserializeOwned>(value: Value) -> Result<T, EventsError> {
    serde_json::from_value(value).map_err(|_| EventsError::UnexpectedResponse)
}

fn timed_out(timeout: Duration) -> EventsError {
    from_service(ServiceError::Timeout { timeout })
}

fn retryable(error: &EventsError) -> bool {
    match error {
        EventsError::Service(error) => matches!(
            error.as_ref(),
            ServiceError::TransportClosed
                | ServiceError::TransportSend(_)
                | ServiceError::Timeout { .. }
                | ServiceError::SubscriptionLagged { .. }
        ),
        // Streamable HTTP also represents request-scoped EOF using -32603.
        EventsError::Remote(error) => error.code == ErrorCode::INTERNAL_ERROR,
        _ => false,
    }
}

fn retry_delay(attempt: u8, minimum: u64) -> Duration {
    let ceiling = (250u64 << attempt.min(5)).max(minimum);
    let jitter = RandomState::new().hash_one(ceiling) % ceiling;
    Duration::from_millis(ceiling + jitter)
}

async fn drain_page(burst: &mut u8) {
    *burst += 1;
    if *burst == POLL_BURST {
        tokio::task::yield_now().await;
        *burst = 0;
    }
}

fn deadline(delay_ms: u64) -> Result<Instant, EventsError> {
    Instant::now()
        .checked_add(Duration::from_millis(delay_ms))
        .ok_or(EventsError::UnexpectedResponse)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn immediate_poll_burst_yields_every_tenth_page() {
        let mut burst = 0;
        for _ in 0..2 {
            for _ in 0..9 {
                let mut page = std::pin::pin!(drain_page(&mut burst));
                assert!(futures::poll!(&mut page).is_ready());
            }
            let mut page = std::pin::pin!(drain_page(&mut burst));
            assert!(futures::poll!(&mut page).is_pending());
            page.await;
        }
    }

    #[test]
    fn jitter_stays_in_exponential_bounds() {
        for minimum in [0, POLL_FLOOR_MS] {
            for attempt in 0..=u8::MAX {
                let base: u128 = (250u64 << attempt.min(5)).max(minimum).into();
                assert!((base..base * 2).contains(&(retry_delay(attempt, minimum).as_millis())));
            }
        }
    }

    #[test]
    fn client_retries_internal_errors_but_not_invalid_or_unknown_rpc_errors() {
        let error = |code| {
            from_service(ServiceError::McpError(crate::ErrorData::new(
                ErrorCode(code),
                "fixture",
                None,
            )))
        };
        assert!(retryable(&error(-32603)));
        for code in [-32023, -32024, -32026, -32602, -32601, -32099] {
            assert!(!retryable(&error(code)), "incorrectly retried {code}");
        }
    }

    #[test]
    fn dedup_evicts_only_the_oldest_of_1024_ids() {
        let mut delivery = Delivery::default();
        let mut cursor = None;
        let occurrence = |id: usize| {
            EventOccurrence::new(
                id.to_string(),
                "source",
                chrono::Utc::now(),
                JsonObject::new(),
            )
        };
        for id in 0..=DEDUP_CAPACITY {
            delivery.event = Some(occurrence(id));
            assert!(matches!(
                delivery.next(&mut cursor),
                Some(EventUpdate::Occurrence(_))
            ));
        }
        delivery.event = Some(occurrence(1));
        assert!(delivery.next(&mut cursor).is_none());
        delivery.event = Some(occurrence(0));
        assert!(matches!(
            delivery.next(&mut cursor),
            Some(EventUpdate::Occurrence(_))
        ));
    }
}
