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

use async_trait::async_trait;
use azure_core::prelude::*;
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::future::FusedFuture;
use futures::stream::{unfold, FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use nativelink_error::{make_err, Code, Error};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{StoreDriver, StoreKey, UploadSizeInfo};
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::sleep;
use tracing::{event, Level};
use uuid::Uuid;

/// Azure Blob Storage has specific limitations and best practices:
/// - Maximum blob size: ~4.75 TB (50,000 blocks of 100 MB each).
/// - Block size must be between 64 KB and 100 MB.
/// - Block blobs are ideal for storing large files that are read sequentially.
///
/// **Reference**: For more details, refer to the [Azure documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/).

// Default buffer size for reading and uploading data to Azure Blob Storage set to 4 MB to balance memory usage and upload efficiency.
// For high-concurrency workloads, reduce this to save memory. For large sequential uploads, consider increasing for better performance.
const BUFFER_SIZE: usize = 4 * 1024 * 1024;

// Block blobs in Azure have a maximum block size of 100 MB. This constant defines the upper limit for block uploads to comply with Azure's specifications.
// Adjust the block size (MAX_BLOCK_SIZE) and buffer size (BUFFER_SIZE) based on the workload. Larger sizes improve performance for large uploads but require more memory.
const MAX_BLOCK_SIZE: usize = 100 * 1024 * 1024;

#[derive(MetricsComponent)]
pub struct AzureStore {
    blob_service_client: Arc<BlobServiceClient>,
    container_name: String,
    retrier: Retrier,
}

impl AzureStore {
    pub async fn new(
        account_name: &str,
        account_key: &str,
        container_name: &str,
    ) -> Result<Arc<Self>, Error> {
        let blob_service_client = Arc::new(
            StorageAccountClient::new_access_key(account_name, account_key).blob_service_client(),
        );

        let container_client = blob_service_client.container_client(container_name);
        container_client
            .get_properties()
            .execute()
            .await
            .map_err(|e| make_err!(Code::InvalidArgument, "Container validation failed: {e:?}"))?;

        let retrier = Retrier::new(
            Arc::new(|duration| {
                Box::pin(sleep(
                    duration + Duration::from_millis(rand::random::<u64>() % 100),
                ))
            }),
            Arc::new(|delay| delay),
            RetryResult::default_retry_policy(),
        );

        Ok(Arc::new(Self {
            blob_service_client,
            container_name: container_name.to_string(),
            retrier,
        }))
    }

    fn make_blob_path(&self, key: &StoreKey<'_>) -> String {
        key.as_str().to_string()
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
        let container_client = self
            .blob_service_client
            .container_client(&self.container_name);
        let blob_client = container_client.blob_client(&self.make_blob_path(key));

        // MD5 Hash during blob uploads verifies content validation.
        blob_client
            .put_block_blob(data)
            .content_length(size as u64)
            .hash(md5)
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
        let container_client = self
            .blob_service_client
            .container_client(&self.container_name);
        let blob_client = container_client.blob_client(&self.make_blob_path(key));

        let mut block_ids = Vec::new();
        let mut block_id_count = 0;

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
                .retry(unfold((), |_| async {
                    let result = blob_client
                        .put_block(block_id.clone(), buffer[..bytes_read].to_vec())
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
                }))
                .await?;

            block_id_count += 1;
        }

        let block_list = block_ids
            .into_iter()
            .map(|id| BlockId::new(id))
            .collect::<Vec<_>>();

        let block_list = Self::sort_block_ids(block_ids);
        blob_client
            .put_block_list(block_list)
            .execute()
            .await
            .map_err(|e| make_err!(Code::Aborted, "Failed to commit blocks: {e:?}"))?;

        event!(Level::INFO, "Uploaded large blob successfully: {:?}", key);
        Ok(())
    }

    // Optimized for high-throughput uploads by using multiple concurrent tasks to upload blocks. Concurrency improves speed but requires sufficient system resources.
    // Block IDs are tracked and committed at the end. Errors during block uploads trigger retries to ensure reliability.
    async fn upload_large_blob_concurrently(
        &self,
        key: &StoreKey<'_>,
        reader: &mut DropCloserReadHalf,
        concurrency: usize,
    ) -> Result<(), Error> {
        if concurrency == 0 {
            return Err(make_err!(
                Code::InvalidArgument,
                "Concurrency must be greater than 0"
            ));
        }

        let container_client = self
            .blob_service_client
            .container_client(&self.container_name);
        let blob_client = container_client.blob_client(&self.make_blob_path(key));
        let block_ids = Arc::new(Mutex::new(Vec::new()));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let mut tasks = Vec::new();

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
            let blob_client = blob_client.clone();
            let block_ids_clone = block_ids.clone();

            let permit =
                semaphore.clone().acquire_owned().await.map_err(|_| {
                    make_err!(Code::Unavailable, "Failed to acquire semaphore permit")
                })?;

            tasks.push(tokio::spawn(async move {
                let _permit = permit;

                blob_client
                    .put_block(block_id.clone(), buffer[..bytes_read].to_vec())
                    .execute()
                    .await
                    .map_err(|e| {
                        event!(Level::ERROR, ?e, "Failed to upload block.");
                        e
                    })?;

                block_ids_clone.lock().await.push(block_id);
                Ok::<(), Error>(())
            }));
        }

        for task in tasks {
            task.await??;
        }

        let sorted_block_ids = {
            let locked_ids = block_ids.lock().await;
            Self::sort_block_ids(locked_ids.clone())
        };
        blob_client
            .put_block_list(sorted_block_ids)
            .execute()
            .await
            .map_err(|e| make_err!(Code::Aborted, "Failed to commit blocks: {e:?}"))?;

        event!(Level::INFO, "Successfully uploaded large blob: {:?}", key);
        Ok(())
    }
}

#[async_trait]
impl StoreDriver for AzureStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        let container_client = self
            .blob_service_client
            .container_client(&self.container_name);

        futures::stream::iter(keys.iter().zip(results.iter_mut()))
            .for_each_concurrent(None, |(key, result)| async {
                let blob_client = container_client.blob_client(&self.make_blob_path(key));
                *result = match self
                    .retrier
                    .retry(unfold((), |_| async {
                        match blob_client.get_properties().execute().await {
                            Ok(properties) => Some((RetryResult::Ok(properties), ())),
                            Err(e) => {
                                if e.is_transient() {
                                    Some((RetryResult::Retry(e), ()))
                                } else {
                                    Some((RetryResult::Err(e), ()))
                                }
                            }
                        }
                    }))
                    .await
                {
                    Ok(properties) => Some(properties.blob.content_length as u64),
                    Err(e) => {
                        event!(
                            Level::ERROR,
                            "Failed to fetch properties for key {:?}: {:?}",
                            key,
                            e
                        );
                        None
                    }
                };
            })
            .await;

        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        match upload_size {
            UploadSizeInfo::ExactSize(size) if size < MAX_BLOCK_SIZE as u64 => {
                let mut data = Vec::new();
                reader
                    .read_to_end(&mut data)
                    .await
                    .map_err(|e| make_err!(Code::Aborted, "Failed to read data: {e:?}"))?;
                self.upload_blob(&key, data, size).await
            }
            _ => {
                self.upload_large_blob_concurrently(&key, &mut reader, 4)
                    .await
            }
        }
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let container_client = self
            .blob_service_client
            .container_client(&self.container_name);
        let blob_client = container_client.blob_client(&self.make_blob_path(key));

        let range_header = Self::format_range(offset, length);
        let response = self
            .retrier
            .retry(unfold((), |_| async {
                let result = blob_client
                    .get()
                    .range(range_header.clone())
                    .execute()
                    .await;
                match result {
                    Ok(response) => Some((RetryResult::Ok(response), ())),
                    Err(e) => {
                        if e.is_transient() {
                            Some((RetryResult::Retry(e), ()))
                        } else {
                            Some((RetryResult::Err(e), ()))
                        }
                    }
                }
            }))
            .await?;

        writer
            .write_all(&response.data)
            .await
            .map_err(|e| make_err!(Code::Aborted, "Failed to write to output: {e:?}"))?;

        writer
            .send_eof()
            .map_err(|e| make_err!(Code::Aborted, "Failed to send EOF: {e:?}"))
    }
}

#[async_trait]
impl HealthStatusIndicator for AzureStore {
    fn get_name(&self) -> &'static str {
        "AzureStore"
    }

    async fn check_health(&self, _namespace: Cow<'static, str>) -> HealthStatus {
        StoreDriver::check_health(Pin::new(self), namespace).await
    }
}
