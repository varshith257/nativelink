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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{cmp, env};

use async_trait::async_trait;
use azure_core::prelude::*;
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::future::FusedFuture;
use futures::stream::{unfold, FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use http_body::{Frame, SizeHint};
use hyper::client::connect::{Connected, Connection, HttpConnector};
use hyper::service::Service;
use hyper::Uri;
use hyper_rustls::{HttpsConnector, MaybeHttpsStream};
use nativelink_config::stores::AzureSpec;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{
    make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf,
};
use nativelink_util::fs;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use rand::rngs::OsRng;
use rand::Rng;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, SemaphorePermit};
use tokio::time::sleep;
use tracing::{event, Level};

use crate::cas_utils::is_zero_digest;

/// Azure Blob Storage has specific limitations and best practices:
/// - Maximum blob size: ~4.75 TB (50,000 blocks of 100 MB each).
/// - Block size must be between 64 KB and 100 MB.
/// - Block blobs are ideal for storing large files that are read sequentially.
///
/// **Reference**: For more details, refer to the [Azure documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/).

// Default buffer size for retrying uploads and reading data in Azure.
const DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST: usize = 5 * 1024 * 1024;

// Block blobs in Azure have a maximum block size of 100 MB. This constant defines the upper limit for block uploads to comply with Azure's specifications.
// Adjust the block size (MAX_BLOCK_SIZE) and buffer size (BUFFER_SIZE) based on the workload. Larger sizes improve performance for large uploads but require more memory.
const MAX_BLOCK_SIZE: usize = 100 * 1024 * 1024;

// Azure minimum block size (for compliance).
const AZURE_MIN_BLOCK_SIZE: usize = 64 * 1024;

// Azure default concurrent uploads for large blob uploads.
const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 10;

// Azure maximum allowed number of blocks per blob.
const AZURE_MAX_UPLOAD_PARTS: usize = 50_000;

#[derive(MetricsComponent)]
pub struct AzureStore {
    container_client: Arc<ContainerClient>,
    now_fn: NowFn,
    #[metric(help = "The Azure container name for the store")]
    container_name: String,
    retrier: Retrier,
    #[metric(help = "The maximum buffer size for retrying uploads")]
    max_retry_buffer_per_request: usize,
    #[metric(help = "The number of concurrent uploads allowed for large blob uploads")]
    max_concurrent_uploads: usize,
    #[metric(help = "The number of seconds to consider an object expired")]
    consider_expired_after_s: i64,
    #[metric(help = "The key prefix for logical separation in Azure blobs")]
    key_prefix: String,
}

impl<I, NowFn> AzureStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    pub async fn new(spec: &AzureSpec) -> Result<Arc<Self>, Error> {
        let jitter_amt = spec.retry.jitter;
        let jitter_fn = Arc::new(move |delay: Duration| {
            if jitter_amt == 0. {
                return delay;
            }
            let min = 1. - (jitter_amt / 2.);
            let max = 1. + (jitter_amt / 2.);
            delay.mul_f32(OsRng.gen_range(min..max))
        });

        let blob_service_client = if let (Some(account_name), Some(account_key)) =
            (&spec.account_name, &spec.account_key)
        {
            StorageAccountClient::new_access_key(account_name, account_key).blob_service_client()
        } else if let Some(sas_token) = &spec.sas_token {
            StorageAccountClient::new_sas_token(sas_token).blob_service_client()
        } else {
            // Fallback to Azure Managed Identity
            StorageAccountClient::new_default().blob_service_client()
        };

        // container_client
        //     .get_properties()
        //     .execute()
        //     .await
        //     .map_err(|e| make_err!(Code::InvalidArgument, "Container validation failed: {e:?}"))?;
        Self::new_with_client_and_jitter(spec, blob_service_client, jitter_fn, now_fn)
    }

    pub fn new_with_client_and_jitter(
        spec: &AzureSpec,
        blob_service_client: ContainerClient,
        jitter_fn: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
        now_fn: NowFn,
    ) -> Result<Arc<Self>, Error> {
        let container_client = blob_service_client.container_client(&spec.container_name);
        tokio::spawn(async move {
            if let Err(e) = container_client.get_properties().execute().await {
                tracing::error!(
                    "Failed to validate Azure container '{}': {:?}",
                    &spec.container_name,
                    e
                );
            }
        });
        let retrier = Retrier::new(
            Arc::new(|duration| Box::pin(sleep(duration))),
            jitter_fn,
            spec.retry.clone(),
        );

        Ok(Arc::new(Self {
            container_client: Arc::new(container_client),
            now_fn,
            container_name: spec.container_name.clone(),
            retrier,
            max_retry_buffer_per_request: spec
                .max_retry_buffer_per_request
                .unwrap_or(DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST),
            max_concurrent_uploads: spec
                .max_concurrent_uploads
                .map_or(DEFAULT_MAX_CONCURRENT_UPLOADS, |v| v),
            consider_expired_after_s: spec.consider_expired_after_s as i64,
            key_prefix: spec.key_prefix.as_ref().unwrap_or(&String::new()).clone(),
        }))
    }

    fn make_blob_path(&self, key: &StoreKey<'_>) -> String {
        format!("{}{}", self.key_prefix, key.as_str())
    }

    /// Generates a valid HTTP range header string for a given offset and length.
    fn format_range(offset: u64, length: Option<u64>) -> String {
        match length {
            Some(len) => format!("bytes={}-{}", offset, offset + len - 1),
            None => format!("bytes={}-", offset),
        }
    }

    /// Sorts block IDs lexicographically, ensuring they meet Azure Blob Storage requirements.
    fn sort_block_ids(block_ids: Vec<String>) -> Vec<BlockId> {
        let mut sorted_ids: Vec<BlockId> = block_ids.into_iter().map(BlockId::new).collect();
        sorted_ids.sort_by(|a, b| a.id.cmp(&b.id));
        sorted_ids
    }

    async fn upload_blob(&self, key: &StoreKey<'_>, data: Vec<u8>, size: u64) -> Result<(), Error> {
        let blob_client = self.container_client.blob_client(self.make_blob_path(key));

        blob_client
            .put_block_blob(data)
            .content_length(size as u64)
            .execute()
            .await
            .map_err(|e| make_err!(Code::Aborted, "Azure Blob upload failed: {e:?}"))?;

        event!(Level::INFO, "Uploaded blob successfully: {:?}", key);
        Ok(())
    }

    async fn download_blob(
        &self,
        key: &StoreKey<'_>,
        range: Option<(u64, u64)>,
    ) -> Result<Bytes, Error> {
        let container_client = self
            .blob_service_client
            .container_client(&self.container_name);
        let blob_client = container_client.blob_client(&self.make_blob_path(key));

        let mut get_blob_request = blob_client.get();

        if let Some((start, end)) = range {
            if end < start {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "Invalid range: start cannot be greater than end"
                ));
            }
            get_blob_request = get_blob_request.range(format!("bytes={}-{}", start, end));
        }

        let response = get_blob_request
            .execute()
            .await
            .map_err(|e| make_err!(Code::NotFound, "Azure Blob download failed: {e:?}"))?;

        event!(Level::INFO, "Downloaded blob successfully: {:?}", key);
        Ok(Bytes::from(response.data))
    }

    // Handles large uploads by splitting data into smaller blocks (up to 100 MB each). Blocks are uploaded sequentially and committed in a single request. Block IDs
    // are sorted lexicographically before submission to meet Azure's requirements.
    // Retries are implemented to handle transient failures. Suitable for cases where concurrency is not needed or memory is constrained.
    async fn upload_large_blob(
        &self,
        key: &StoreKey<'_>,
        reader: &mut DropCloserReadHalf,
    ) -> Result<(), Error> {
        let blob_client = self.container_client.blob_client(self.make_blob_path(key));

        let bytes_per_block = (max_size / (MAX_BLOCK_SIZE as u64))
            .clamp(AZURE_MIN_BLOCK_SIZE as u64, MAX_BLOCK_SIZE as u64);

        let mut block_ids = Vec::new();

        loop {
            let mut buffer = vec![0u8; BUFFER_SIZE];
            let bytes_read = reader
                .read(&mut buffer)
                .await
                .map_err(|e| make_err!(Code::Aborted, "Failed to read data: {e:?}"))?;

            if bytes_read == 0 {
                break;
            }

            let block_id = base64::encode(Uuid::new_v4().as_bytes());
            block_ids.push(block_id.clone());

            self.retrier
                .retry(unfold(
                    buffer[..bytes_read].to_vec(),
                    move |data| async move {
                        let result = blob_client
                            .put_block(block_id.clone(), data.clone())
                            .execute()
                            .await;

                        match result {
                            Ok(_) => Some((RetryResult::Ok(()), ())),
                            Err(e) => Some((
                                RetryResult::Retry(make_err!(
                                    Code::Aborted,
                                    "Failed to upload block: {e:?}"
                                )),
                                (),
                            )),
                        }
                    },
                ))
                .await?;

            block_id_count += 1;
        }

        let block_list = Self::sort_block_ids(block_ids);

        blob_client
            .put_block_list(block_list)
            .execute()
            .await
            .map_err(|e| make_err!(Code::Aborted, "Failed to commit blocks: {e:?}"))?;

        event!(Level::INFO, "Uploaded large blob successfully: {:?}", key);
        Ok(())
    }
}

#[async_trait]
impl<I, NowFn> StoreDriver for AzureStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        keys.iter()
            .zip(results.iter_mut())
            .map(|(key, result)| async move {
                // Handle zero digest
                if is_zero_digest(key.borrow()) {
                    *result = Some(0);
                    return Ok::<_, Error>(());
                }

                // Fetch blob properties
                let blob_client = self.container_client.blob_client(self.make_blob_path(key));
                match self
                    .retrier
                    .retry(unfold((), |_| async {
                        match blob_client.get_properties().execute().await {
                            Ok(properties) => Some((RetryResult::Ok(properties), ())),
                            Err(e) if e.is_transient() => Some((RetryResult::Retry(e), ())),
                            Err(e) => Some((RetryResult::Err(e), ())),
                        }
                    }))
                    .await
                {
                    Ok(properties) => *result = Some(properties.blob.content_length as u64),
                    Err(e) => {
                        event!(
                            Level::ERROR,
                            "Failed to fetch properties for key {:?}: {:?}",
                            key,
                            e
                        );
                        *result = None;
                    }
                }
                Ok::<_, Error>(())
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let blob_path = self.make_blob_path(&key);

        let max_size = match upload_size {
            UploadSizeInfo::ExactSize(sz) | UploadSizeInfo::MaxSize(sz) => sz,
        };
        // Single-part upload for small-sized data
        if max_size < MAX_BLOCK_SIZE as u64 && matches!(upload_size, UploadSizeInfo::ExactSize(_)) {
            let UploadSizeInfo::ExactSize(size) = upload_size else {
                unreachable!("upload_size must be UploadSizeInfo::ExactSize here");
            };
            let mut data = Vec::new();
            reader
                .read_to_end(&mut data)
                .await
                .map_err(|e| make_err!(Code::Aborted, "Failed to read data: {e:?}"))?;
            return self.upload_blob(&key, data, size).await;
        }
        // Multipart upload for larger data
        self.upload_large_blob(&key, &mut reader, max_size).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            writer
                .send_eof()
                .map_err(|e| make_err!(Code::Aborted, "Failed to send EOF: {e:?}"))?;
            return Ok(());
        }

        let blob_client = self.container_client.blob_client(self.make_blob_path(&key));
        let range_header = Self::format_range(offset, length);

        self.retrier
            .retry(unfold(writer, move |writer| async move {
                let response = blob_client
                    .get()
                    .range(range_header.clone())
                    .execute()
                    .await;

                let mut stream = match response {
                    Ok(resp) => resp.data.into_stream(),
                    Err(e) => {
                        if e.is_transient() {
                            return Some((RetryResult::Retry(e), writer));
                        }
                        return Some((
                            RetryResult::Err(make_err!(
                                Code::Unavailable,
                                "Failed to fetch blob data: {e:?}"
                            )),
                            writer,
                        ));
                    }
                };

                // Stream the data to the writer
                while let Some(result) = stream.next().await {
                    match result {
                        Ok(bytes) => {
                            if bytes.is_empty() {
                                continue;
                            }
                            writer.send(bytes).await.map_err(|e| {
                                make_err!(Code::Aborted, "Failed to write to output: {e:?}")
                            })?;
                        }
                        Err(e) => {
                            return Some((
                                RetryResult::Retry(make_err!(
                                    Code::Aborted,
                                    "Error reading blob stream: {e:?}"
                                )),
                                writer,
                            ));
                        }
                    }
                }

                // Send EOF after streaming
                writer
                    .send_eof()
                    .map_err(|e| make_err!(Code::Aborted, "Failed to send EOF: {e:?}"))?;

                Some((RetryResult::Ok(()), writer))
            }))
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
impl<I, NowFn> HealthStatusIndicator for AzureStore<NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + Unpin + 'static,
{
    fn get_name(&self) -> &'static str {
        "AzureStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
