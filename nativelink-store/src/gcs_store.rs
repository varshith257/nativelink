// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::FusedFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt};

use nativelink_config::stores::GCSSpec;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf,
};
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use serde::{Deserialize, Serialize};

use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::Error as GcsError;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{event, Level};

use rand::random;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

// Minimum and maximum size for GCS multipart uploads.
const MIN_MULTIPART_SIZE: u64 = 5 * 1024 * 1024;
const MAX_MULTIPART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub struct GCSStore {
    bucket: String,
    key_prefix: String,
    gcs_client: Arc<Client>,
    retrier: Retrier,
}

impl GCSStore {
    pub async fn new(spec: &GCSSpec) -> Result<Arc<Self>, Error> {
        let client = ClientConfig::default()
            .with_auth_path(spec.service_account_key_file.clone())
            .await
            .map(Client::new)
            .map(Arc::new)
            .map_err(|e| make_err!(Code::Unavailable, "Failed to initialize GCS client: {e:?}"))?;

        let retrier = Retrier::new(
            Arc::new(move |duration| {
                // Jitter: +/-50% random variation
                // This helps distribute retries more evenly and prevents synchronized bursts.
                // Reference: https://cloud.google.com/storage/docs/retry-strategy#exponential-backoff
                let jitter = rand::random::<f32>() * 0.5;
                let backoff_with_jitter = duration.mul_f32(1.0 + jitter);
                Box::pin(tokio::time::sleep(backoff_with_jitter))
            }),
            Arc::new(|delay| {
                // Exponential backoff: Multiply delay by 2, with an upper cap
                let max_delay = Duration::from_secs(30);
                delay.mul_f32(2.0).min(max_delay)
            }),
            spec.retry.clone(),
        );

        Ok(Arc::new(Self {
            bucket: spec.bucket.clone(),
            key_prefix: spec.key_prefix.clone().unwrap_or_default(),
            gcs_client: client,
            retrier,
        }))
    }

    fn make_gcs_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str())
    }
}

#[async_trait]
impl StoreDriver for GCSStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        if keys.len() != results.len() {
            return Err(make_err!(
                Code::InvalidArgument,
                "Mismatched lengths: keys = {}, results = {}",
                keys.len(),
                results.len()
            ));
        }

        let fetches = keys
            .iter()
            .map(|key| {
                let object_name = self.make_gcs_path(key);
                let bucket = self.bucket.clone();
                let client = self.gcs_client.clone();

                async move {
                    let req = GetObjectRequest {
                        bucket,
                        object: object_name,
                        ..Default::default()
                    };

                    match client.object().get(req).await {
                        Ok(metadata) => Ok(Some(metadata.size)),
                        Err(google_cloud_storage::Error::NotFound(_)) => Ok(None),
                        Err(e) => Err(make_err!(
                            Code::Unavailable,
                            "Failed to check existence of object: {e:?}"
                        )),
                    }
                }
            })
            .collect::<FuturesUnordered<_>>();

        for (result, fetch_result) in results.iter_mut().zip(fetches.collect::<Vec<_>>().await) {
            *result = fetch_result?;
        }

        Ok(())
    }

    /// Updates a file in GCS using resumable uploads.
    ///
    /// GCS Resumable Uploads Note:
    /// Resumable upload sessions in GCS do not require explicit abort calls for cleanup.
    /// GCS automatically deletes incomplete sessions after a configurable period (default: 1 week).
    /// For best practices, ensure that session URLs are stored if uploads may need to resume later.
    /// Reference: https://cloud.google.com/storage/docs/resumable-uploads
    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let object_name = self.make_gcs_path(&key);

        let buffer_size = match upload_size {
            UploadSizeInfo::ExactSize(size) => size.min(MIN_MULTIPART_SIZE).max(64 * 1024),
            _ => MIN_MULTIPART_SIZE,
        } as usize;

        let upload_request = UploadObjectRequest {
            bucket: self.bucket.clone(),
            object: object_name.clone(),
            ..Default::default()
        };

        debug!("Starting upload to GCS for object: {}", object_name);

        let mut bytes_uploaded = 0;
        let media = Media {
            name: object_name.clone().into(),
            content_type: "application/octet-stream".into(),
            content_length: match upload_size {
                UploadSizeInfo::ExactSize(size) => Some(size),
                _ => None,
            },
        };

        let session_url = self
            .retrier
            .retry(async {
                let request_builder =
                    google_cloud_storage::http::objects::upload::build_resumable_session_simple(
                        &self.gcs_client.base_url,
                        &self.gcs_client.http,
                        &upload_request,
                        &media,
                    );
                let response = self.gcs_client.send(request_builder.build()?).await?;
                response
                    .headers()
                    .get("location")
                    .ok_or_else(|| {
                        make_err!(Code::Unavailable, "Failed to get resumable session URL")
                    })
                    .and_then(|value| {
                        value
                            .to_str()
                            .map_err(|e| make_err!(Code::Internal, "Invalid URL: {e:?}"))
                    })
                    .map(|url| url.to_string())
            })
            .await?;

        let mut buffer = vec![0; buffer_size];
        let mut offset = 0;

        loop {
            let bytes_read = reader
                .read(&mut buffer)
                .await
                .map_err(|e| make_err!(Code::Unavailable, "Failed to read input stream: {e:?}"))?;

            if bytes_read == 0 {
                break;
            }

            let chunk_data = &buffer[..bytes_read];
            let upload_range = format!(
                "bytes {}-{}/{}",
                offset,
                offset + bytes_read as u64 - 1,
                "*"
            );

            debug!(
                "Uploading chunk: offset={}, size={}, range={}",
                offset, bytes_read, upload_range
            );

            self.retrier
                .retry(async {
                    let chunk_request = self
                        .gcs_client
                        .http
                        .put(&session_url)
                        .body(chunk_data.to_vec())
                        .header("Content-Range", upload_range.clone());
                    let response = chunk_request.send().await?;
                    if response.status().is_success() || response.status().as_u16() == 308 {
                        Ok::<(), Error>(())
                    } else {
                        Err(make_err!(
                            Code::Unavailable,
                            "Failed to upload chunk: HTTP {}",
                            response.status()
                        ))
                    }
                })
                .await?;

            offset += bytes_read as u64;
        }

        debug!("Upload to GCS completed for object: {}", object_name);

        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let object_name = self.make_gcs_path(&key);

        let req = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: object_name,
            ..Default::default()
        };

        let range = Range {
            start: Some(offset),
            end: length
                .and_then(|len| offset.checked_add(len))
                .ok_or_else(|| {
                    make_err!(
                        Code::InvalidArgument,
                        "Invalid range calculation for object: {}",
                        object_name
                    )
                })?,
        };

        self.retrier
            .retry(async {
                let mut stream = self
                    .gcs_client
                    .download_streamed_object(&req, &range)
                    .await
                    .map_err(|e| {
                        make_err!(
                            Code::Unavailable,
                            "Failed to initiate streaming download from GCS: {e:?}"
                        )
                    })?;
                while let Some(chunk_result) = stream.next().await {
                    match chunk_result {
                        Ok(chunk) => {
                            writer.write_all(&chunk).await.map_err(|e| {
                                make_err!(
                                    Code::Unavailable,
                                    "Failed to write downloaded chunk: {e:?}"
                                )
                            })?;
                        }
                        Err(e) => {
                            return Err(make_err!(
                                Code::Unavailable,
                                "Error during streaming download from GCS: {e:?}"
                            ));
                        }
                    }
                }

                writer
                    .send_eof()
                    .map_err(|e| make_err!(Code::Internal, "Failed to send EOF: {e:?}"))?;

                Ok::<(), Error>(())
            })
            .await
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }
}

#[async_trait]
impl HealthStatusIndicator for GCSStore {
    fn get_name(&self) -> &'static str {
        "GCSStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
