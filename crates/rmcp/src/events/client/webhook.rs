use super::*;
use crate::{
    events::webhook::{
        SUBSCRIBE, SubscribeDelivery, SubscribeMode, SubscribeParams, UNSUBSCRIBE,
        UnsubscribeDelivery, UnsubscribeParams, WebhookOptions, WebhookSubscription,
    },
    model::{JsonObject, ResultType},
};

impl EventsClient {
    /// Subscribe or refresh the same `(principal, url, name, arguments)` grant.
    ///
    /// Stage the secret at the endpoint before calling. Reuse the last safe
    /// checkpoint and refresh before the returned deadline. This low-level
    /// method does not run a receiver, schedule refreshes or persist cursors.
    pub async fn subscribe_webhook(
        &self,
        name: impl Into<String>,
        arguments: Option<JsonObject>,
        options: WebhookOptions,
    ) -> Result<WebhookSubscription, EventsError> {
        let params = SubscribeParams {
            name: name.into(),
            arguments,
            delivery: SubscribeDelivery {
                mode: SubscribeMode::Webhook,
                url: options.url,
                secret: options.secret.encode().to_string(),
            },
            cursor: options.cursor,
            max_age_ms: options.max_age_ms,
            ttl_ms: options.ttl,
        };
        let result: WebhookSubscription = decode_result(
            self.webhook_request(SUBSCRIBE, &params.name, &params)
                .await?,
        )?;
        result
            .result_type
            .is_complete()
            .then_some(result)
            .ok_or(EventsError::UnexpectedResponse)
    }

    /// End the webhook grant selected by the same key used to subscribe.
    pub async fn unsubscribe_webhook(
        &self,
        name: impl Into<String>,
        arguments: Option<JsonObject>,
        url: impl Into<String>,
    ) -> Result<(), EventsError> {
        let params = UnsubscribeParams {
            name: name.into(),
            arguments,
            delivery: UnsubscribeDelivery { url: url.into() },
        };
        unsubscribe_result(
            self.webhook_request(UNSUBSCRIBE, &params.name, &params)
                .await?,
        )
    }

    async fn webhook_request(
        &self,
        method: &'static str,
        name: &str,
        params: &impl Serialize,
    ) -> Result<ServerResult, EventsError> {
        self.require_advertisement()?;
        let body = encode_params(params)?;
        self.with_route(name, |descriptor| {
            let body = body.clone();
            async move {
                if method == SUBSCRIBE && !descriptor.delivery.contains(&DeliveryMode::Webhook) {
                    return Err(unsupported(DeliveryMode::Webhook));
                }
                self.request(route(method, body, descriptor.input_schema))
                    .await
            }
        })
        .await
    }
}

fn unsubscribe_result(result: ServerResult) -> Result<(), EventsError> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Complete {
        #[serde(default)]
        result_type: ResultType,
        #[serde(rename = "_meta")]
        _meta: Option<crate::model::MetaObject>,
    }
    let result_type = match result {
        ServerResult::EmptyResult(_) => return Ok(()),
        ServerResult::TaskAckResult(result) => result.result_type,
        result => decode_result::<Complete>(result)?.result_type,
    };
    result_type
        .is_complete()
        .then_some(())
        .ok_or(EventsError::UnexpectedResponse)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_standard_empty_completion() {
        for wire in [
            json!({}),
            json!({"_meta":{"hint":true}}),
            json!({"resultType":"complete"}),
            json!({"resultType":"complete", "_meta":{"hint":true}}),
        ] {
            assert!(unsubscribe_result(serde_json::from_value(wire.clone()).unwrap()).is_ok());
            assert!(
                unsubscribe_result(ServerResult::CustomResult(crate::model::CustomResult(wire)))
                    .is_ok()
            );
        }
        for wire in [
            json!({"resultType":"task"}),
            json!({"resultType":"complete","unexpected":42}),
        ] {
            assert!(matches!(
                unsubscribe_result(serde_json::from_value(wire).unwrap()),
                Err(EventsError::UnexpectedResponse)
            ));
        }
    }
}
