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

use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest};
use google_cloud_storage::http::resumable_upload_client::ChunkSize;
use google_cloud_storage::http::resumable_upload_client::ResumableUploadClient;
use google_cloud_storage::http::resumable_upload_client::UploadStatus;
use nativelink_config::stores::GCSSpec;
use nativelink_error::{make_err, Code, Error};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use rand::random;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use reqwest_middleware::{ClientWithMiddleware as Client, RequestBuilder};
use tracing::debug;

// Minimum size for GCS multipart uploads.
// Note: If you change this, adjust the docs in the config.
const MIN_MULTIPART_SIZE: u64 = 5 * 1024 * 1024;

// Maximum size for GCS multipart uploads.
// Note: If you change this, adjust the docs in the config.
const MAX_MULTIPART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

// Note: If you change this, adjust the docs in the config.
const DEFAULT_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

#[derive(MetricsComponent)]
pub struct GCSStore {
    #[metric(help = "The bucket name used for GCSStore")]
    bucket: String,
    #[metric(help = "The key prefix used for objects in GCSStore")]
    key_prefix: String,
    #[metric(help = "Number of retry attempts in GCS operations")]
    retry_count: usize,
    #[metric(help = "Total bytes uploaded to GCS")]
    uploaded_bytes: u64,
    #[metric(help = "Total bytes downloaded from GCS")]
    downloaded_bytes: u64,
    gcs_client: Arc<Client>,
    retrier: Retrier,
}

impl GCSStore {
    pub async fn new(spec: &GCSSpec) -> Result<Arc<Self>, Error> {
        let client = ClientConfig::default()
            .with_auth()
            .await
            .map(Client::new)
            .map(Arc::new)
            .map_err(|e| make_err!(Code::Unavailable, "Failed to initialize GCS client: {e:?}"))?;

        let retry_jitter = spec.retry.jitter;
        let retry_delay = spec.retry.delay;

        let retrier = Retrier::new(
            Arc::new(move |duration| {
                // Jitter: +/-50% random variation
                // This helps distribute retries more evenly and prevents synchronized bursts.
                // Reference: https://cloud.google.com/storage/docs/retry-strategy#exponential-backoff
                let jitter = random::<f32>() * (retry_jitter / 2.0);
                let backoff_with_jitter = duration.mul_f32(1.0 + jitter);
                Box::pin(tokio::time::sleep(backoff_with_jitter))
            }),
            Arc::new(move |delay| {
                // Exponential backoff: Multiply delay by 2, with an upper cap
                let retry_delay = retry_delay;
                let exponential_backoff = delay.mul_f32(2.0);
                Duration::from_secs_f32(retry_delay).min(exponential_backoff)
            }),
            spec.retry.clone(),
        );

        Ok(Arc::new(Self {
            bucket: spec.bucket.clone(),
            key_prefix: spec.key_prefix.clone().unwrap_or_default(),
            gcs_client: client,
            retrier,
            retry_count: 0,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
        }))
    }
    fn make_gcs_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str())
    }

    fn build_resumable_session_simple(
        &self,
        base_url: &str,
        client: &Client,
        req: &UploadObjectRequest,
        media: &Media,
    ) -> RequestBuilder {
        let url = format!(
            "{}/b/{}/o?uploadType=resumable",
            base_url,
            utf8_percent_encode(&req.bucket, NON_ALPHANUMERIC)
        );
        let mut builder = client
            .post(url)
            .query(req)
            .query(&[("name", &media.name)])
            .header(CONTENT_TYPE, media.content_type.to_string())
            .header(CONTENT_LENGTH, "0");

        if let Some(content_length) = media.content_length {
            builder = builder.header("X-Upload-Content-Length", content_length.to_string());
        }

        builder
    }

    pub async fn start_resumable_upload(
        &self,
        bucket: &str,
        object_name: &str,
        content_length: Option<u64>,
    ) -> Result<ResumableUploadClient, Error> {
        let media = Media {
            name: object_name.into(),
            content_type: "application/octet-stream".into(),
            content_length,
        };

        let request = self.build_resumable_session_simple(
            "https://storage.googleapis.com",
            &self.gcs_client,
            &UploadObjectRequest {
                bucket: bucket.to_string(),
                ..Default::default()
            },
            &media,
        );

        let response = request
            .send()
            .await
            .map_err(|e| make_err!(Code::Unavailable, "Failed to start resumable upload: {e:?}"))?;

        if let Some(location) = response.headers().get(LOCATION) {
            let session_url = location
                .to_str()
                .map_err(|_| make_err!(Code::Unavailable, "Invalid session URL"))?
                .to_string();

            Ok(ResumableUploadClient::new(
                session_url,
                self.gcs_client.clone(),
            ))
        } else {
            Err(make_err!(
                Code::Unavailable,
                "No Location header in response"
            ))
        }
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

                    match client.get_object(&req).await {
                        Ok(metadata) => match metadata.size.try_into() {
                            Ok(size) => Ok(Some(size)),
                            Err(_) => Err(make_err!(
                                Code::Internal,
                                "Invalid object size: {}",
                                metadata.size
                            )),
                        },
                        Err(e) => {
                            if e.to_string().contains("404") {
                                Ok(None)
                            } else {
                                Err(make_err!(
                                    Code::Unavailable,
                                    "Failed to check existence of object: {e:?}"
                                ))
                            }
                        }
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

        let file_size = match upload_size {
            UploadSizeInfo::ExactSize(size) => size,
            _ => return Err(make_err!(Code::InvalidArgument, "Unknown file size")),
        } as usize;

        debug!("Starting upload to GCS for object: {}", object_name);

        let session_url = self
            .start_resumable_upload(&self.bucket, &object_name, Some(file_size as u64))
            .await
            .map_err(|e| make_err!(Code::Unavailable, "Failed to start resumable upload: {e:?}"))?;

        let resumable_client =
            ResumableUploadClient::new(session_url.clone(), self.gcs_client.clone());

        self.retrier
            .retry(stream::once(async {
                let result = async {
                    match resumable_client.status(None).await? {
                        UploadStatus::NotStarted => {
                            // Single chunk upload for small files
                            resumable_client
                                .upload_single_chunk(reader.clone(), file_size as usize)
                                .await?;
                        }
                        UploadStatus::ResumeIncomplete(range) => {
                            // Resumable chunked upload for large files
                            let total_size = range.total_size.unwrap_or(file_size);
                            let mut current_position = range.last_uploaded_byte.unwrap_or(0);

                            while current_position < total_size {
                                let chunk_size = ChunkSize::new(
                                    current_position,
                                    (current_position + DEFAULT_CHUNK_SIZE - 1).min(total_size - 1),
                                    Some(total_size),
                                );

                                debug!(
                                    "Uploading chunk: {}-{} for object: {}",
                                    chunk_size.first_byte, chunk_size.last_byte, object_name
                                );

                                resumable_client
                                    .upload_multiple_chunk(&mut reader, &chunk_size)
                                    .await?;

                                current_position += chunk_size.size();
                            }
                        }
                        UploadStatus::Ok(_) => {
                            debug!("Upload completed!");
                            return Ok(());
                        }
                    }
                    Ok::<(), Error>(())
                }
                .await;

                match result {
                    Ok(_) => RetryResult::Ok(()),
                    Err(e) => {
                        debug!("Retryable upload error: {:?}", e);
                        RetryResult::Retry(e)
                    }
                }
            }))
            .await?;

        debug!("Upload completed for object: {}", object_name);
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
            object: object_name.clone(),
            ..Default::default()
        };

        let range = Range(Some(offset), length.map(|len| offset + len));

        self.retrier
            .retry(stream::once(async {
                let result = async {
                    let mut stream = self
                        .gcs_client
                        .download_streamed_object(&req, &range)
                        .await
                        .map_err(|e| {
                            make_err!(Code::Unavailable, "Failed to initiate download: {e:?}")
                        })?;

                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.map_err(|e| {
                            make_err!(Code::Unavailable, "Failed to download chunk: {e:?}")
                        })?;
                        writer
                            .send(chunk)
                            .await
                            .map_err(|e| make_err!(Code::Unavailable, "Write error: {e:?}"))?;
                    }

                    writer
                        .send_eof()
                        .map_err(|e| make_err!(Code::Internal, "EOF error: {e:?}"))?;
                    Ok::<(), Error>(())
                }
                .await;

                match result {
                    Ok(_) => RetryResult::Ok(()),
                    Err(e) => RetryResult::Retry(e),
                }
            }))
            .await?;

        Ok(())
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
